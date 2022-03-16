package goparquet

import (
	"bytes"
	"io"
	"math/rand"
	"testing"

	"github.com/fraugster/parquet-go/parquetschema"
	"github.com/stretchr/testify/require"
)

func buildTestStream(t *testing.T) []byte {
	schema, err := parquetschema.ParseSchemaDefinition(`message msg {
  required int64 a;
  required int64 b;
  optional group x {
    required int64 c;
    required int64 d;
  }
  required group y {
     required int64 e;
  }
}
`)
	require.NoError(t, err)
	buf := &bytes.Buffer{}
	pw := NewFileWriter(buf, WithSchemaDefinition(schema))
	for i := 0; i < 10000; i++ {
		data := map[string]any{
			"a": rand.Int63(),
			"b": rand.Int63(),
			"x": map[string]any{
				"c": rand.Int63(),
				"d": rand.Int63(),
			},
			"y": map[string]any{
				"e": rand.Int63(),
			},
		}
		require.NoError(t, pw.AddData(data))
		if i%100 == 0 {
			require.NoError(t, pw.FlushRowGroup())
		}
	}
	require.NoError(t, pw.Close())
	return buf.Bytes()
}

func TestByteReaderSelected(t *testing.T) {
	r := buildTestStream(t)
	pr, err := NewFileReader(bytes.NewReader(r), "a")
	require.NoError(t, err)

	for {
		data, err := pr.NextRow()
		if err == io.EOF {
			break
		}
		require.NoError(t, err)
		require.Equal(t, 2, len(data))
		_, ok := data["a"]
		require.True(t, ok)
		y, ok := data["y"]
		require.True(t, ok)
		require.Empty(t, y)
	}
}

func TestByteReaderSelectedInner(t *testing.T) {
	r := buildTestStream(t)
	pr, err := NewFileReader(bytes.NewReader(r), "x.c")
	require.NoError(t, err)

	for {
		data, err := pr.NextRow()
		if err == io.EOF {
			break
		}
		require.NoError(t, err)
		require.Equal(t, 2, len(data))
		x, ok := data["x"].(map[string]any)
		require.True(t, ok)
		require.Equal(t, 1, len(x))
		y, ok := data["y"]
		require.True(t, ok)
		require.Empty(t, y)
	}
}

func TestByteReaderSelectedInnerByColumnPath(t *testing.T) {
	r := buildTestStream(t)
	pr, err := NewFileReaderWithOptions(bytes.NewReader(r), WithColumnPaths(ColumnPath{"x", "c"}))
	require.NoError(t, err)

	for {
		data, err := pr.NextRow()
		if err == io.EOF {
			break
		}
		require.NoError(t, err)
		require.Equal(t, 2, len(data))
		x, ok := data["x"].(map[string]any)
		require.True(t, ok)
		require.Equal(t, 1, len(x))
		y, ok := data["y"]
		require.True(t, ok)
		require.Empty(t, y)
	}
}

func TestByteReaderSelectedInnerFull(t *testing.T) {
	r := buildTestStream(t)
	pr, err := NewFileReader(bytes.NewReader(r), "x")
	require.NoError(t, err)

	require.NotNil(t, pr.GetColumnByName("x.c"))

	for {
		data, err := pr.NextRow()
		if err == io.EOF {
			break
		}
		require.NoError(t, err)
		require.Equal(t, 2, len(data))
		x, ok := data["x"].(map[string]any)
		require.True(t, ok)
		require.Equal(t, 2, len(x))
		y, ok := data["y"]
		require.True(t, ok)
		require.Empty(t, y)
	}
}

func TestByteReaderSelectedInnerFullByColumnPath(t *testing.T) {
	r := buildTestStream(t)
	pr, err := NewFileReaderWithOptions(bytes.NewReader(r), WithColumnPaths(ColumnPath{"x"}))
	require.NoError(t, err)

	require.NotNil(t, pr.GetColumnByPath(ColumnPath{"x", "c"}))

	for {
		data, err := pr.NextRow()
		if err == io.EOF {
			break
		}
		require.NoError(t, err)
		require.Equal(t, 2, len(data))
		x, ok := data["x"].(map[string]any)
		require.True(t, ok)
		require.Equal(t, 2, len(x))
		y, ok := data["y"]
		require.True(t, ok)
		require.Empty(t, y)
	}
}

func TestByteReaderSelectedInnerFullSetSelectedColumns(t *testing.T) {
	r := buildTestStream(t)
	pr, err := NewFileReaderWithOptions(bytes.NewReader(r))
	require.NoError(t, err)

	pr.SetSelectedColumns("x")

	for {
		data, err := pr.NextRow()
		if err == io.EOF {
			break
		}
		require.NoError(t, err)
		require.Equal(t, 2, len(data))
		x, ok := data["x"].(map[string]any)
		require.True(t, ok)
		require.Equal(t, 2, len(x))
		y, ok := data["y"]
		require.True(t, ok)
		require.Empty(t, y)
	}
}

func TestByteReaderSelectedInnerFullSetSelectedColumnsByPath(t *testing.T) {
	r := buildTestStream(t)
	pr, err := NewFileReaderWithOptions(bytes.NewReader(r))
	require.NoError(t, err)

	pr.SetSelectedColumnsByPath(ColumnPath{"x"})

	for {
		data, err := pr.NextRow()
		if err == io.EOF {
			break
		}
		require.NoError(t, err)
		require.Equal(t, 2, len(data))
		x, ok := data["x"].(map[string]any)
		require.True(t, ok)
		require.Equal(t, 2, len(x))
		y, ok := data["y"]
		require.True(t, ok)
		require.Empty(t, y)
	}
}

func TestIssue60(t *testing.T) {
	sd, err := parquetschema.ParseSchemaDefinition(`message test {
		required group population (LIST){
			repeated group list {
				optional int64 element;
			}
		}
	}`)
	require.NoError(t, err)

	var buf bytes.Buffer
	fw := NewFileWriter(&buf, WithSchemaDefinition(sd))

	err = fw.AddData(map[string]any{
		"population": map[string]any{
			"list": []map[string]any{
				{"element": int64(23)},
				{"element": nil},
				{"element": int64(42)},
			},
		},
	})
	require.NoError(t, err)

	require.NoError(t, fw.Close())

	r, err := NewFileReader(bytes.NewReader(buf.Bytes()))
	require.NoError(t, err)

	row, err := r.NextRow()
	require.NoError(t, err)

	require.Equal(t, map[string]any{
		"population": map[string]any{
			"list": []map[string]any{
				{"element": int64(23)},
				{},
				{"element": int64(42)},
			},
		},
	}, row)

	t.Logf("row = %#v", row)
}
