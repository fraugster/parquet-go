package goparquet

import (
	"math/rand"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/fraugster/parquet-go/parquet"
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
		sf.Col.reset(parquet.FieldRepetitionType_REQUIRED)
		for i := range arr {
			b, err := sf.Col.add(arr[i], 0, 0, 0)

			require.True(t, b)
			require.NoError(t, err)
		}
		require.Equal(t, size, sf.Col.values.size)
	}
}
