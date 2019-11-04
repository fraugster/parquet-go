package goparquet

import (
	"bytes"
	"fmt"
	"io"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/fraugster/parquet-go/parquet"
)

/*
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

	fmt.Printf("%s", r.schemaReader.String())
}
*/

func TestWriteThenReadFile(t *testing.T) {
	testFunc := func(opts ...FileWriterOption) {
		_ = os.Mkdir("files", 0755)

		wf, err := os.OpenFile("files/test.parquet", os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0644)
		require.NoError(t, err, "creating file failed")

		w := NewFileWriter(wf, opts...)

		fooStore, err := NewInt64Store(parquet.Encoding_PLAIN, true, &ColumnParameters{})
		require.NoError(t, err, "failed to create fooStore")

		barStore, err := NewByteArrayStore(parquet.Encoding_PLAIN, true, &ColumnParameters{})
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

			require.NoError(t, w.AddData(map[string]interface{}{"foo": int64(idx), "bar": []byte("value" + fmt.Sprint(idx))}), "%d. AddData failed", idx)
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
			require.NoError(t, r.readRowGroup(), "Reading row group failed")
			for i := 0; i < int(r.rowGroupNumRecords()); i++ {
				data, err := r.getData()
				require.NoError(t, err)
				_, ok := data["foo"]
				require.True(t, ok)
			}
		}
	}

	testFunc(CompressionCodec(parquet.CompressionCodec_SNAPPY), CreatedBy("parquet-go-unittest"))
	testFunc(CompressionCodec(parquet.CompressionCodec_SNAPPY), CreatedBy("parquet-go-unittest"), WithDataPageV2())
}

func TestWriteThenReadFileRepeated(t *testing.T) {
	_ = os.Mkdir("files", 0755)

	wf, err := os.OpenFile("files/test.parquet", os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0644)
	require.NoError(t, err, "creating file failed")

	w := NewFileWriter(wf, CompressionCodec(parquet.CompressionCodec_SNAPPY), CreatedBy("parquet-go-unittest"))

	fooStore, err := NewInt64Store(parquet.Encoding_PLAIN, true, &ColumnParameters{})
	require.NoError(t, err, "failed to create fooStore")

	require.NoError(t, w.AddColumn("foo", NewDataColumn(fooStore, parquet.FieldRepetitionType_REPEATED)))

	data := []map[string]interface{}{
		{"foo": []int64{1}},
		{"foo": []int64{1, 2, 3, 1}},
		{},
		{"foo": []int64{1, 3, 1, 1}},
		{},
		{"foo": []int64{1, 2, 2, 1}},
	}

	for i := range data {
		require.NoError(t, w.AddData(data[i]))
	}

	assert.NoError(t, w.Close(), "Close failed")

	require.NoError(t, wf.Close())

	rf, err := os.Open("files/test.parquet")
	require.NoError(t, err, "opening file failed")
	defer rf.Close()

	r, err := NewFileReader(rf)
	require.NoError(t, err, "creating file reader failed")
	require.NoError(t, r.readRowGroup())

	require.Equal(t, int64(len(data)), r.rowGroupNumRecords())
	for i := range data {
		d, err := r.getData()
		require.NoError(t, err)
		require.Equal(t, data[i], d)
	}
}

func TestWriteThenReadFileOptional(t *testing.T) {
	_ = os.Mkdir("files", 0755)

	wf, err := os.OpenFile("files/test.parquet", os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0644)
	require.NoError(t, err, "creating file failed")

	w := NewFileWriter(wf, CompressionCodec(parquet.CompressionCodec_SNAPPY), CreatedBy("parquet-go-unittest"))

	fooStore, err := NewByteArrayStore(parquet.Encoding_PLAIN, true, &ColumnParameters{})
	require.NoError(t, err, "failed to create fooStore")

	require.NoError(t, w.AddColumn("foo", NewDataColumn(fooStore, parquet.FieldRepetitionType_OPTIONAL)))

	data := []map[string]interface{}{
		{"foo": []byte("1")},
		{"foo": []byte("2")},
		{},
		{"foo": []byte("3")},
		{},
		{"foo": []byte("4")},
	}

	for i := range data {
		require.NoError(t, w.AddData(data[i]))
	}

	assert.NoError(t, w.Close(), "Close failed")

	require.NoError(t, wf.Close())

	rf, err := os.Open("files/test.parquet")
	require.NoError(t, err, "opening file failed")
	defer rf.Close()

	r, err := NewFileReader(rf)
	require.NoError(t, err, "creating file reader failed")
	require.NoError(t, r.readRowGroup())

	require.Equal(t, int64(len(data)), r.rowGroupNumRecords())
	root := r.schemaReader.(*schema).root
	for i := range data {
		_, ok := data[i]["foo"]
		rL, dL, b := root.getFirstRDLevel()
		if ok {
			assert.False(t, b)
			assert.Equal(t, int32(0), rL)
			assert.Equal(t, int32(1), dL)
		} else {
			assert.False(t, b)
			assert.Equal(t, int32(-1), rL)
			assert.Equal(t, int32(-1), dL)
		}

		get, err := r.getData()
		require.NoError(t, err)
		require.Equal(t, data[i], get)
	}
}

func TestWriteThenReadFileNested(t *testing.T) {
	_ = os.Mkdir("files", 0755)

	wf, err := os.OpenFile("files/test.parquet", os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0644)
	require.NoError(t, err, "creating file failed")

	w := NewFileWriter(wf, CompressionCodec(parquet.CompressionCodec_SNAPPY), CreatedBy("parquet-go-unittest"))

	fooStore, err := NewInt64Store(parquet.Encoding_PLAIN, true, &ColumnParameters{})
	require.NoError(t, err, "failed to create fooStore")
	barStore, err := NewByteArrayStore(parquet.Encoding_PLAIN, true, &ColumnParameters{})
	require.NoError(t, err, "failed to create barStore")

	require.NoError(t, w.AddGroup("baz", parquet.FieldRepetitionType_REPEATED))
	require.NoError(t, w.AddColumn("baz.foo", NewDataColumn(fooStore, parquet.FieldRepetitionType_REQUIRED)))
	require.NoError(t, w.AddColumn("baz.bar", NewDataColumn(barStore, parquet.FieldRepetitionType_OPTIONAL)))

	data := []map[string]interface{}{
		{
			"baz": []map[string]interface{}{
				{"foo": int64(10)},
			},
		},
	}

	for i := range data {
		require.NoError(t, w.AddData(data[i]))
	}

	assert.NoError(t, w.Close(), "Close failed")

	require.NoError(t, wf.Close())

	rf, err := os.Open("files/test.parquet")
	require.NoError(t, err, "opening file failed")
	defer rf.Close()

	r, err := NewFileReader(rf)
	require.NoError(t, err, "creating file reader failed")
	require.NoError(t, r.readRowGroup())

	require.Equal(t, int64(len(data)), r.rowGroupNumRecords())
	for i := range data {
		d, err := r.getData()
		require.NoError(t, err)
		require.Equal(t, data[i], d)
	}
}

func TestWriteThenReadFileNested2(t *testing.T) {
	_ = os.Mkdir("files", 0755)

	wf, err := os.OpenFile("files/test.parquet", os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0644)
	require.NoError(t, err, "creating file failed")

	w := NewFileWriter(wf, CompressionCodec(parquet.CompressionCodec_SNAPPY), CreatedBy("parquet-go-unittest"))

	blaStore, err := NewInt64Store(parquet.Encoding_PLAIN, true, &ColumnParameters{})
	require.NoError(t, err, "failed to create fooStore")
	barStore, err := NewByteArrayStore(parquet.Encoding_PLAIN, true, &ColumnParameters{})
	require.NoError(t, err, "failed to create barStore")

	require.NoError(t, w.AddGroup("foo", parquet.FieldRepetitionType_REPEATED))
	require.NoError(t, w.AddColumn("foo.bla", NewDataColumn(blaStore, parquet.FieldRepetitionType_REQUIRED)))
	require.NoError(t, w.AddColumn("foo.bar", NewDataColumn(barStore, parquet.FieldRepetitionType_OPTIONAL)))

	data := []map[string]interface{}{
		{
			"foo": []map[string]interface{}{
				{
					"bla": int64(23),
					"bar": []byte("foobar"),
				},
			},
		},
		{
			"foo": []map[string]interface{}{
				{
					"bla": int64(24),
					"bar": []byte("hello"),
				},
			},
		},
		{
			"foo": []map[string]interface{}{
				{
					"bla": int64(25),
				},
				{
					"bla": int64(26),
					"bar": []byte("bye!"),
				},
				{
					"bla": int64(27),
				},
			},
		},
	}
	for i := range data {
		require.NoError(t, w.AddData(data[i]))
	}

	assert.NoError(t, w.Close(), "Close failed")

	require.NoError(t, wf.Close())

	rf, err := os.Open("files/test.parquet")
	require.NoError(t, err, "opening file failed")
	defer rf.Close()

	r, err := NewFileReader(rf)
	require.NoError(t, err, "creating file reader failed")
	require.NoError(t, r.readRowGroup())

	require.Equal(t, int64(len(data)), r.rowGroupNumRecords())
	for i := range data {
		d, err := r.getData()
		require.NoError(t, err)
		require.Equal(t, data[i], d)
	}
}

func TestWriteThenReadFileMap(t *testing.T) {
	_ = os.Mkdir("files", 0755)

	wf, err := os.OpenFile("files/test.parquet", os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0644)
	require.NoError(t, err, "creating file failed")

	w := NewFileWriter(wf, CompressionCodec(parquet.CompressionCodec_SNAPPY), CreatedBy("parquet-go-unittest"))

	fooStore, err := NewInt64Store(parquet.Encoding_PLAIN, true, &ColumnParameters{})
	require.NoError(t, err, "failed to create fooStore")
	barStore, err := NewByteArrayStore(parquet.Encoding_PLAIN, true, &ColumnParameters{})
	require.NoError(t, err, "failed to create barStore")
	elementStore, err := NewInt32Store(parquet.Encoding_PLAIN, true, &ColumnParameters{})
	require.NoError(t, err, "failed to create elementStore")

	elementCol := NewDataColumn(elementStore, parquet.FieldRepetitionType_REQUIRED)
	list, err := NewListColumn(elementCol, parquet.FieldRepetitionType_OPTIONAL)
	require.NoError(t, err)

	require.NoError(t, w.AddColumn("foo", NewDataColumn(fooStore, parquet.FieldRepetitionType_REQUIRED)))
	require.NoError(t, w.AddColumn("bar", NewDataColumn(barStore, parquet.FieldRepetitionType_OPTIONAL)))
	require.NoError(t, w.AddColumn("baz", list))

	/* `message test_msg {
		required int64 foo;
		optional binary bar (STRING);
		optional group baz (LIST) {
			repeated group list {
				required int32 element;
			}
		}
	}` */
	data := []map[string]interface{}{
		{
			"foo": int64(500),
		},
		{
			"foo": int64(23),
			"bar": []byte("hello!"),
			"baz": map[string]interface{}{
				"list": []map[string]interface{}{
					{"element": int32(23)},
				},
			},
		},
		{
			"foo": int64(42),
			"bar": []byte("world!"),
			"baz": map[string]interface{}{
				"list": []map[string]interface{}{
					{"element": int32(1)},
					{"element": int32(1)},
					{"element": int32(2)},
					{"element": int32(3)},
					{"element": int32(5)},
				},
			},
		},
		{
			"foo": int64(1000),
			"bar": []byte("bye!"),
			"baz": map[string]interface{}{
				"list": []map[string]interface{}{
					{"element": int32(2)},
					{"element": int32(3)},
					{"element": int32(5)},
					{"element": int32(7)},
					{"element": int32(11)},
				},
			},
		},
	}

	for i := range data {
		require.NoError(t, w.AddData(data[i]))
	}

	assert.NoError(t, w.Close(), "Close failed")

	require.NoError(t, wf.Close())

	rf, err := os.Open("files/test.parquet")
	require.NoError(t, err, "opening file failed")
	defer rf.Close()

	r, err := NewFileReader(rf)
	require.NoError(t, err, "creating file reader failed")
	require.NoError(t, r.readRowGroup())

	require.Equal(t, int64(len(data)), r.rowGroupNumRecords())
	for i := range data {
		d, err := r.getData()
		require.NoError(t, err)
		require.Equal(t, data[i], d)
	}
}

func TestWriteThenReadFileNested3(t *testing.T) {
	_ = os.Mkdir("files", 0755)

	wf, err := os.OpenFile("files/test.parquet", os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0644)
	require.NoError(t, err, "creating file failed")

	w := NewFileWriter(wf, CompressionCodec(parquet.CompressionCodec_SNAPPY), CreatedBy("parquet-go-unittest"))
	valueStore, err := NewInt64Store(parquet.Encoding_PLAIN, true, &ColumnParameters{})
	require.NoError(t, err, "failed to create valueStore")
	require.NoError(t, w.AddGroup("baz", parquet.FieldRepetitionType_OPTIONAL))
	require.NoError(t, w.AddColumn("baz.value", NewDataColumn(valueStore, parquet.FieldRepetitionType_REQUIRED)))

	data := []map[string]interface{}{
		{
			"baz": map[string]interface{}{
				"value": int64(9001),
			},
		},
		{},
		{},
	}

	for i := range data {
		require.NoError(t, w.AddData(data[i]))
	}

	assert.NoError(t, w.Close(), "Close failed")

	require.NoError(t, wf.Close())

	rf, err := os.Open("files/test.parquet")
	require.NoError(t, err, "opening file failed")
	defer rf.Close()

	r, err := NewFileReader(rf)
	require.NoError(t, err, "creating file reader failed")
	require.NoError(t, r.readRowGroup())

	require.Equal(t, int64(len(data)), r.rowGroupNumRecords())
	for i := range data {
		d, err := r.getData()
		require.NoError(t, err)
		require.Equal(t, data[i], d)
	}
}

func TestWriteEmptyDict(t *testing.T) {
	_ = os.Mkdir("files", 0755)

	wf, err := os.OpenFile("files/test.parquet", os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0644)
	require.NoError(t, err, "creating file failed")

	w := NewFileWriter(wf, CompressionCodec(parquet.CompressionCodec_SNAPPY), CreatedBy("parquet-go-unittest"))
	valueStore, err := NewByteArrayStore(parquet.Encoding_PLAIN, true, &ColumnParameters{})
	require.NoError(t, err, "failed to create valueStore")
	require.NoError(t, w.AddColumn("value", NewDataColumn(valueStore, parquet.FieldRepetitionType_OPTIONAL)))

	for i := 0; i < 1000; i++ {
		require.NoError(t, w.AddData(nil))
	}

	assert.NoError(t, w.Close(), "Close failed")

	require.NoError(t, wf.Close())

	rf, err := os.Open("files/test.parquet")
	require.NoError(t, err, "opening file failed")
	defer rf.Close()

	r, err := NewFileReader(rf)
	require.NoError(t, err, "creating file reader failed")
	require.NoError(t, r.readRowGroup())

	require.Equal(t, int64(1000), r.rowGroupNumRecords())
	for i := 0; i < 1000; i++ {
		d, err := r.getData()
		require.NoError(t, err)
		require.Equal(t, map[string]interface{}{}, d)
	}
}

func TestReadWriteMultiLevel(t *testing.T) {
	sc := `message txn {
  optional group cluster (LIST) {
    repeated group list {
      required group element {
        optional group cluster_step (LIST) {
            repeated group list {
              required group element {
                optional group story_point {
                  required binary type (STRING);
                }
              }
            }
          }
      }
    }
  }
}
`
	buf := &bytes.Buffer{}
	sd, err := ParseSchemaDefinition(sc)
	require.NoError(t, err)
	w := NewFileWriter(buf, UseSchemaDefinition(sd))

	require.NoError(t, w.AddData(map[string]interface{}{}))
	require.NoError(t, w.Close())
	buf2 := bytes.NewReader(buf.Bytes())
	r, err := NewFileReader(buf2)
	require.NoError(t, err)
	data, err := r.NextRow()
	require.NoError(t, err)
	require.Equal(t, map[string]interface{}{}, data)

	_, err = r.NextRow()
	require.Equal(t, io.EOF, err)
}
