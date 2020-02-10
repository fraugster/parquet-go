package goparquet

import (
	"bytes"
	"io"
	"math/rand"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/fraugster/parquet-go/parquet"
	"github.com/fraugster/parquet-go/parquetschema"
)

var sizeFixture = []struct {
	Col      *ColumnStore
	Generate func(n int) ([]interface{}, int64)
}{
	{
		Col: func() *ColumnStore {
			n, err := NewInt32Store(parquet.Encoding_PLAIN, true, &ColumnParameters{})
			if err != nil {
				panic(err)
			}
			return n
		}(),
		Generate: func(n int) ([]interface{}, int64) {
			ret := make([]interface{}, 0, n)
			var size int64
			for i := 0; i < n; i++ {
				ret = append(ret, rand.Int31())
				size += 4
			}

			return ret, size
		},
	},

	{
		Col: func() *ColumnStore {
			n, err := NewInt64Store(parquet.Encoding_PLAIN, true, &ColumnParameters{})
			if err != nil {
				panic(err)
			}
			return n
		}(),
		Generate: func(n int) ([]interface{}, int64) {
			ret := make([]interface{}, 0, n)
			var size int64
			for i := 0; i < n; i++ {
				ret = append(ret, rand.Int63())
				size += 8
			}

			return ret, size
		},
	},

	{
		Col: func() *ColumnStore {
			n, err := NewByteArrayStore(parquet.Encoding_PLAIN, true, &ColumnParameters{})
			if err != nil {
				panic(err)
			}
			return n
		}(),
		Generate: func(n int) ([]interface{}, int64) {
			ret := make([]interface{}, 0, n)
			var size int64
			for i := 0; i < n; i++ {
				s := rand.Int63n(32)
				data := make([]byte, s)
				ret = append(ret, data)
				size += s
			}

			return ret, size
		},
	},
}

func TestColumnSize(t *testing.T) {
	for _, sf := range sizeFixture {
		arr, size := sf.Generate(rand.Intn(1000) + 1)
		sf.Col.reset(parquet.FieldRepetitionType_REQUIRED, 0, 0)
		for i := range arr {
			err := sf.Col.add(arr[i], 0, 0, 0)
			require.NoError(t, err)
		}
		require.Equal(t, size, sf.Col.values.size)
	}
}

func TestSchemaCopy(t *testing.T) {
	schema := `message txn {
  optional boolean is_fraud;
}`
	def, err := parquetschema.ParseSchemaDefinition(schema)
	require.NoError(t, err)
	buf := &bytes.Buffer{}
	writer := NewFileWriter(buf, WithSchemaDefinition(def))

	for i := 0; i < 3; i++ {
		var d interface{}
		switch {
		case i%3 == 0:
			d = true
		case i%3 == 1:
			d = false
		case i%3 == 2:
			d = nil
		}
		require.NoError(t, writer.AddData(map[string]interface{}{
			"is_fraud": d,
		}))
	}

	require.NoError(t, writer.Close())

	buf2 := bytes.NewReader(buf.Bytes())
	buf3 := &bytes.Buffer{}
	reader, err := NewFileReader(buf2)
	require.NoError(t, err)
	writer2 := NewFileWriter(buf3, WithSchemaDefinition(reader.GetSchemaDefinition()))

	for {
		rec, err := reader.NextRow()
		if err == io.EOF {
			break
		}
		require.NoError(t, err)
		err = writer2.AddData(rec)

		require.NoError(t, err)
	}

	require.NoError(t, writer2.Close())
}
