package go_parquet

import (
	"fmt"
	"os"
	"testing"

	"github.com/fraugster/parquet-go/parquet"
)

func TestWriteThenReadFile(t *testing.T) {
	os.Mkdir("files", 0755)

	wf, err := os.OpenFile("files/test.parquet", os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0644)
	if err != nil {
		t.Fatalf("creating file failed: %v", err)
	}

	w := NewFileWriter(wf, 0)

	fooStore, err := NewInt64Store(parquet.Encoding_PLAIN, true)
	if err != nil {
		t.Fatalf("failed to create fooStore: %v", err)
	}

	barStore, err := NewByteArrayStore(parquet.Encoding_PLAIN, true)
	if err != nil {
		t.Fatalf("failed to create barStore: %v", err)
	}

	w.AddColumn("foo", NewDataColumn(fooStore, parquet.FieldRepetitionType_REQUIRED))
	w.AddColumn("bar", NewDataColumn(barStore, parquet.FieldRepetitionType_OPTIONAL))

	const (
		numRecords = 10000
		flushLimit = 1000
	)

	for idx := 0; idx < numRecords; idx++ {
		if idx > 0 && idx%flushLimit == 0 {
			if err := w.FlushRowGroup(parquet.CompressionCodec_SNAPPY); err != nil {
				t.Fatalf("Flushing row group failed: %v", err)
			}
		}

		if err := w.AddData(map[string]interface{}{"foo": int64(idx), "bar": []byte(fmt.Sprint(idx))}); err != nil {
			t.Fatalf("%d. AddData failed: %v", idx, err)
		}

	}

	if err := w.FlushRowGroup(parquet.CompressionCodec_SNAPPY); err != nil {
		t.Fatalf("Flushing row group failed: %v", err)
	}

	if err := w.Close(); err != nil {
		t.Logf("Close failed: %v", err)
	}

	wf.Close()

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
