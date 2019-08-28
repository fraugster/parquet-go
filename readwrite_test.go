package goparquet

import (
	"fmt"
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

	fmt.Printf("%s", r.SchemaReader.String())
}
*/

func TestWriteThenReadFile(t *testing.T) {
	_ = os.Mkdir("files", 0755)

	wf, err := os.OpenFile("files/test.parquet", os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0644)
	require.NoError(t, err, "creating file failed")

	w := NewFileWriter(wf, CompressionCodec(parquet.CompressionCodec_SNAPPY), CreatedBy("parquet-go-unittest"))

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
		require.NoError(t, r.ReadRowGroup(), "Reading row group failed")
		for i := 0; i < int(r.NumRecords()); i++ {
			data, err := r.GetData()
			require.NoError(t, err)
			_, ok := data["foo"]
			require.True(t, ok)
		}
	}
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
	require.NoError(t, r.ReadRowGroup())

	require.Equal(t, int64(len(data)), r.NumRecords())
	for i := range data {
		d, err := r.GetData()
		require.NoError(t, err)
		require.Equal(t, data[i], d)
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
	require.NoError(t, r.ReadRowGroup())

	require.Equal(t, int64(len(data)), r.NumRecords())
	for i := range data {
		d, err := r.GetData()
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
	require.NoError(t, r.ReadRowGroup())

	require.Equal(t, int64(len(data)), r.NumRecords())
	for i := range data {
		d, err := r.GetData()
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
	require.NoError(t, r.ReadRowGroup())

	require.Equal(t, int64(len(data)), r.NumRecords())
	for i := range data {
		d, err := r.GetData()
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
	require.NoError(t, r.ReadRowGroup())

	require.Equal(t, int64(len(data)), r.NumRecords())
	for i := range data {
		d, err := r.GetData()
		require.NoError(t, err)
		require.Equal(t, data[i], d)
	}
}
