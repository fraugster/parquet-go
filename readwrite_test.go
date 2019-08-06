package go_parquet

import (
	"fmt"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/stretchr/testify/require"

	"github.com/fraugster/parquet-go/parquet"
)

func TestWriteThenReadFile(t *testing.T) {
	_ = os.Mkdir("files", 0755)

	wf, err := os.OpenFile("files/test.parquet", os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0644)
	require.NoError(t, err, "creating file failed")

	w := NewFileWriter(wf, 0)

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
			require.NoError(t, w.FlushRowGroup(parquet.CompressionCodec_SNAPPY), "%d. AddData failed", idx)
		}

		require.NoError(t, w.AddData(map[string]interface{}{"foo": int64(idx), "bar": "value" + fmt.Sprint(idx)}), "%d. AddData failed", idx)
	}

	require.NoError(t, w.FlushRowGroup(parquet.CompressionCodec_SNAPPY), "Flushing row group failed")

	assert.NoError(t, w.Close(), "Close failed")

	require.NoError(t, wf.Close())

	rf, err := os.Open("files/test.parquet")
	if err != nil {
		t.Fatalf("opening file failed: %v", err)
	}
	defer rf.Close()

	r, err := NewFileReader(rf)
	if err != nil {
		t.Fatalf("creating file reader failed: %v", err)
	}

	cols := r.Columns()

	if len(cols) != 2 {
		t.Fatalf("expected 2 columns, got %d instead", len(cols))
	}

	if cols[0].Name() != "foo" {
		t.Errorf("column 0 doesn't have the name foo")
	}
	if cols[0].FlatName() != "foo" {
		t.Errorf("column 0 doesn't have the flat name foo")
	}
	if cols[1].Name() != "bar" {
		t.Errorf("column 0 doesn't have the name bar")
	}
	if cols[1].FlatName() != "bar" {
		t.Errorf("column 0 doesn't have the flat name bar")
	}

	t.Logf("raw group count: %d", r.RawGroupCount())

	for g := 0; g < r.RawGroupCount(); g++ {
		if err := r.ReadRowGroup(); err != nil {
			t.Fatalf("Reading row group failed: %v", err)
		}

		t.Logf("row group %d, got %d records", g, r.NumRecords())

		for i := 0; i < int(r.NumRecords()); i++ {
			data, err := r.GetData()
			if err != nil {
				t.Fatalf("getting record %d failed: %v", i, err)
			}
			if _, ok := data["foo"]; !ok {
				t.Errorf("record doesn't contain expected field foo: %#v", data)
			}
		}
	}
}
