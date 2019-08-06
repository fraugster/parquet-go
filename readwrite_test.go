package go_parquet

import (
	"fmt"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/stretchr/testify/require"

	"github.com/fraugster/parquet-go/parquet"
)

func TestReadFile(t *testing.T) {
	rf, err := os.Open("files/pilot_random.parquet")
	if err != nil {
		t.Fatalf("opening file failed: %v", err)
	}
	defer rf.Close()

	r, err := NewFileReader(rf)
	if err != nil {
		t.Fatalf("creating file reader failed: %v", err)
	}

	fmt.Printf("%s", r.SchemaReader.String())
}

func TestWriteThenReadFile(t *testing.T) {
	_ = os.Mkdir("files", 0755)

	wf, err := os.OpenFile("files/test.parquet", os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0644)
	require.NoError(t, err, "creating file failed")

	w := NewFileWriter(wf, CompressionCodec(parquet.CompressionCodec_SNAPPY), CreatedBy("parquet-go-unittest"))

	fooStore, err := NewInt64Store(parquet.Encoding_PLAIN, true)
	require.NoError(t, err, "failed to create fooStore")

	barStore, err := NewStringStore(parquet.Encoding_PLAIN, true)
	require.NoError(t, err, "failed to create barStore")

	require.NoError(t, w.AddColumn("foo", NewDataColumn(fooStore, parquet.FieldRepetitionType_REQUIRED)))
	require.NoError(t, w.AddColumn("bar", NewDataColumn(barStore, parquet.FieldRepetitionType_OPTIONAL)))

	const (
		numRecords = 10000
		flushLimit = 1000
	)

	for idx := 0; idx < numRecords; idx++ {
		if idx > 0 && idx%flushLimit == 0 {
			require.NoError(t, w.FlushRowGroup(), "%d. AddData failed", idx)
		}

		require.NoError(t, w.AddData(map[string]interface{}{"foo": int64(idx), "bar": "value" + fmt.Sprint(idx)}), "%d. AddData failed", idx)
	}

	assert.NoError(t, w.Close(), "Close failed")

	require.NoError(t, wf.Close())

	rf, err := os.Open("files/test.parquet")
	require.NoError(t, err, "opening file failed")
	defer rf.Close()

	r, err := NewFileReader(rf)
	require.NoError(t, err, "creating file reader failed")

	cols := r.Columns()
	require.Len(t, cols, 2, fmt.Sprintf("expected 2 columns, got %d instead", len(cols)))
	require.Equal(t, "foo", cols[0].Name())
	require.Equal(t, "foo", cols[0].FlatName())
	require.Equal(t, "bar", cols[1].Name())
	require.Equal(t, "bar", cols[1].FlatName())
	for g := 0; g < r.RawGroupCount(); g++ {
		require.NoError(t, r.ReadRowGroup(), "Reading row group failed")
		for i := 0; i < int(r.NumRecords()); i++ {
			data, err := r.GetData()
			require.NoError(t, err)
			_, ok := data["foo"]
			require.True(t, ok)
		}
	}
}
