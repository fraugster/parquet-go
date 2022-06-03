package goparquet

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"math"
	"math/rand"
	"os"
	"testing"
	"time"

	"github.com/fraugster/parquet-go/parquet"
	"github.com/fraugster/parquet-go/parquetschema"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestWriteThenReadFile(t *testing.T) {
	ctx := context.Background()

	testFunc := func(t *testing.T, name string, opts []FileWriterOption, ropts []FileReaderOption) {
		_ = os.Mkdir("files", 0755)

		filename := fmt.Sprintf("files/test1_%s.parquet", name)

		wf, err := os.OpenFile(filename, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0644)
		require.NoError(t, err, "creating file failed")

		w := NewFileWriter(wf, opts...)

		fooStore, err := NewInt64Store(parquet.Encoding_PLAIN, true, &ColumnParameters{})
		require.NoError(t, err, "failed to create fooStore")

		barStore, err := NewByteArrayStore(parquet.Encoding_PLAIN, true, &ColumnParameters{})
		require.NoError(t, err, "failed to create barStore")

		bazStore, err := NewInt32Store(parquet.Encoding_PLAIN, true, &ColumnParameters{})
		require.NoError(t, err, "failed to create bazStore")

		require.NoError(t, w.AddColumn("foo", NewDataColumn(fooStore, parquet.FieldRepetitionType_REQUIRED)))
		require.NoError(t, w.AddColumn("bar", NewDataColumn(barStore, parquet.FieldRepetitionType_OPTIONAL)))
		require.NoError(t, w.AddColumn("baz", NewDataColumn(bazStore, parquet.FieldRepetitionType_OPTIONAL)))

		const (
			numRecords = 10000
			flushLimit = 1000
		)

		for idx := 0; idx < numRecords; idx++ {
			if idx > 0 && idx%flushLimit == 0 {
				require.NoError(t, w.FlushRowGroup(), "%d. AddData failed", idx)
			}

			data := map[string]interface{}{"foo": int64(idx), "bar": []byte("value" + fmt.Sprint(idx))}
			if idx%20 != 0 {
				data["baz"] = int32(idx % 16)
			}

			require.NoError(t, w.AddData(data), "%d. AddData failed", idx)
		}

		assert.NoError(t, w.Close(), "Close failed")

		require.NoError(t, wf.Close())

		rf, err := os.Open(filename)
		require.NoError(t, err, "opening file failed")
		defer rf.Close()

		r, err := NewFileReaderWithOptions(rf, ropts...)
		require.NoError(t, err, "creating file reader failed")

		cols := r.Columns()
		require.Len(t, cols, 3, "got %d column", len(cols))
		require.Equal(t, "foo", cols[0].Name())
		require.Equal(t, "foo", cols[0].FlatName())
		require.Equal(t, "bar", cols[1].Name())
		require.Equal(t, "bar", cols[1].FlatName())
		require.Equal(t, "baz", cols[2].Name())
		require.Equal(t, "baz", cols[2].FlatName())
		for g := 0; g < r.RowGroupCount(); g++ {
			require.NoError(t, r.readRowGroup(ctx), "Reading row group failed")
			for i := 0; i < int(r.schemaReader.rowGroupNumRecords()); i++ {
				data, err := r.schemaReader.getData()
				require.NoError(t, err)
				_, ok := data["foo"]
				require.True(t, ok)
			}
		}
	}

	tests := []struct {
		Name      string
		WriteOpts []FileWriterOption
		ReadOpts  []FileReaderOption
	}{
		{
			Name: "datapagev1",
			WriteOpts: []FileWriterOption{
				WithCompressionCodec(parquet.CompressionCodec_SNAPPY),
				WithCreator("parquet-go-unittest"),
			},
			ReadOpts: []FileReaderOption{},
		},
		{
			Name: "datapagev2",
			WriteOpts: []FileWriterOption{
				WithCompressionCodec(parquet.CompressionCodec_SNAPPY),
				WithCreator("parquet-go-unittest"), WithDataPageV2(),
			},
			ReadOpts: []FileReaderOption{},
		},
		{
			Name: "datapagev1_crc",
			WriteOpts: []FileWriterOption{
				WithCompressionCodec(parquet.CompressionCodec_SNAPPY),
				WithCreator("parquet-go-unittest"),
				WithCRC(true),
			},
			ReadOpts: []FileReaderOption{WithCRC32Validation(true)},
		},
		{
			Name: "datapagev2_crc",
			WriteOpts: []FileWriterOption{
				WithCompressionCodec(parquet.CompressionCodec_SNAPPY),
				WithCreator("parquet-go-unittest"),
				WithDataPageV2(),
				WithCRC(true),
			},
			ReadOpts: []FileReaderOption{WithCRC32Validation(true)},
		},
	}

	for _, tt := range tests {
		t.Run(tt.Name, func(t *testing.T) {
			testFunc(t, tt.Name, tt.WriteOpts, tt.ReadOpts)
		})
	}
}

func TestWriteThenReadFileRepeated(t *testing.T) {
	ctx := context.Background()

	_ = os.Mkdir("files", 0755)

	wf, err := os.OpenFile("files/test2.parquet", os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0644)
	require.NoError(t, err, "creating file failed")

	w := NewFileWriter(wf, WithCompressionCodec(parquet.CompressionCodec_SNAPPY), WithCreator("parquet-go-unittest"))

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

	rf, err := os.Open("files/test2.parquet")
	require.NoError(t, err, "opening file failed")
	defer rf.Close()

	r, err := NewFileReader(rf)
	require.NoError(t, err, "creating file reader failed")
	require.NoError(t, r.readRowGroup(ctx))

	require.Equal(t, int64(len(data)), r.schemaReader.rowGroupNumRecords())
	for i := range data {
		d, err := r.schemaReader.getData()
		require.NoError(t, err)
		require.Equal(t, data[i], d)
	}
}

func TestWriteThenReadFileOptional(t *testing.T) {
	ctx := context.Background()
	_ = os.Mkdir("files", 0755)

	wf, err := os.OpenFile("files/test3.parquet", os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0644)
	require.NoError(t, err, "creating file failed")

	w := NewFileWriter(wf, WithCompressionCodec(parquet.CompressionCodec_SNAPPY), WithCreator("parquet-go-unittest"))

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

	rf, err := os.Open("files/test3.parquet")
	require.NoError(t, err, "opening file failed")
	defer rf.Close()

	r, err := NewFileReader(rf)
	require.NoError(t, err, "creating file reader failed")
	require.NoError(t, r.readRowGroup(ctx))

	require.Equal(t, int64(len(data)), r.schemaReader.rowGroupNumRecords())
	root := r.schemaReader.root
	for i := range data {
		_, ok := data[i]["foo"]
		rL, dL, b := root.getFirstRDLevel()
		if ok {
			assert.False(t, b)
			assert.Equal(t, int32(0), rL)
			assert.Equal(t, int32(1), dL)
		} else {
			assert.False(t, b)
			assert.Equal(t, int32(0), rL)
			assert.Equal(t, int32(0), dL)
		}

		get, err := r.schemaReader.getData()
		require.NoError(t, err)
		require.Equal(t, data[i], get)
	}
}

func TestWriteThenReadFileNested(t *testing.T) {
	ctx := context.Background()
	_ = os.Mkdir("files", 0755)

	wf, err := os.OpenFile("files/test4.parquet", os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0644)
	require.NoError(t, err, "creating file failed")

	w := NewFileWriter(wf, WithCompressionCodec(parquet.CompressionCodec_SNAPPY), WithCreator("parquet-go-unittest"))

	fooStore, err := NewInt64Store(parquet.Encoding_PLAIN, true, &ColumnParameters{})
	require.NoError(t, err, "failed to create fooStore")
	barStore, err := NewByteArrayStore(parquet.Encoding_PLAIN, true, &ColumnParameters{})
	require.NoError(t, err, "failed to create barStore")

	require.NoError(t, w.AddGroupByPath(ColumnPath{"baz"}, parquet.FieldRepetitionType_REPEATED))
	require.NoError(t, w.AddColumnByPath(ColumnPath{"baz", "foo"}, NewDataColumn(fooStore, parquet.FieldRepetitionType_REQUIRED)))
	require.NoError(t, w.AddColumnByPath(ColumnPath{"baz", "bar"}, NewDataColumn(barStore, parquet.FieldRepetitionType_OPTIONAL)))

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

	rf, err := os.Open("files/test4.parquet")
	require.NoError(t, err, "opening file failed")
	defer rf.Close()

	r, err := NewFileReader(rf)
	require.NoError(t, err, "creating file reader failed")
	require.NoError(t, r.readRowGroup(ctx))

	require.Equal(t, int64(len(data)), r.schemaReader.rowGroupNumRecords())
	for i := range data {
		d, err := r.schemaReader.getData()
		require.NoError(t, err)
		require.Equal(t, data[i], d)
	}
}

func TestWriteThenReadFileNested2(t *testing.T) {
	ctx := context.Background()
	_ = os.Mkdir("files", 0755)

	wf, err := os.OpenFile("files/test5.parquet", os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0644)
	require.NoError(t, err, "creating file failed")

	w := NewFileWriter(wf, WithCompressionCodec(parquet.CompressionCodec_SNAPPY), WithCreator("parquet-go-unittest"))

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

	rf, err := os.Open("files/test5.parquet")
	require.NoError(t, err, "opening file failed")
	defer rf.Close()

	r, err := NewFileReader(rf)
	require.NoError(t, err, "creating file reader failed")
	require.NoError(t, r.readRowGroup(ctx))

	require.Equal(t, int64(len(data)), r.schemaReader.rowGroupNumRecords())
	for i := range data {
		d, err := r.schemaReader.getData()
		require.NoError(t, err)
		require.Equal(t, data[i], d)
	}
}

func TestWriteThenReadFileMap(t *testing.T) {
	ctx := context.Background()
	_ = os.Mkdir("files", 0755)

	wf, err := os.OpenFile("files/test6.parquet", os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0644)
	require.NoError(t, err, "creating file failed")

	w := NewFileWriter(wf, WithCompressionCodec(parquet.CompressionCodec_SNAPPY), WithCreator("parquet-go-unittest"))

	fooStore, err := NewInt64Store(parquet.Encoding_PLAIN, true, &ColumnParameters{})
	require.NoError(t, err, "failed to create fooStore")
	barStore, err := NewByteArrayStore(parquet.Encoding_PLAIN, true, &ColumnParameters{})
	require.NoError(t, err, "failed to create barStore")
	elementStore, err := NewInt32Store(parquet.Encoding_PLAIN, true, &ColumnParameters{})
	require.NoError(t, err, "failed to create elementStore")

	elementCol := NewDataColumn(elementStore, parquet.FieldRepetitionType_REQUIRED)
	list, err := NewListColumn(elementCol, parquet.FieldRepetitionType_OPTIONAL)
	require.NoError(t, err)

	quuxParams := &ColumnParameters{
		LogicalType: parquet.NewLogicalType(),
	}
	quuxParams.LogicalType.DECIMAL = parquet.NewDecimalType()
	quuxParams.LogicalType.DECIMAL.Scale = 3
	quuxParams.LogicalType.DECIMAL.Precision = 5

	quuxStore, err := NewInt32Store(parquet.Encoding_PLAIN, true, quuxParams)
	require.NoError(t, err)

	require.NoError(t, w.AddColumn("foo", NewDataColumn(fooStore, parquet.FieldRepetitionType_REQUIRED)))
	require.NoError(t, w.AddColumn("bar", NewDataColumn(barStore, parquet.FieldRepetitionType_OPTIONAL)))
	require.NoError(t, w.AddColumn("baz", list))
	require.NoError(t, w.AddColumn("quux", NewDataColumn(quuxStore, parquet.FieldRepetitionType_OPTIONAL)))

	/* `message test_msg {
		required int64 foo;
		optional binary bar (STRING);
		optional group baz (LIST) {
			repeated group list {
				required int32 element;
			}
		}
		optional int32 quux (DECIMAL(3, 5));
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
			"quux": int32(123456),
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

	rf, err := os.Open("files/test6.parquet")
	require.NoError(t, err, "opening file failed")
	defer rf.Close()

	r, err := NewFileReader(rf)
	require.NoError(t, err, "creating file reader failed")
	require.NoError(t, r.readRowGroup(ctx))

	require.Equal(t, int64(len(data)), r.schemaReader.rowGroupNumRecords())
	for i := range data {
		d, err := r.schemaReader.getData()
		require.NoError(t, err)
		require.Equal(t, data[i], d)
	}
}

func TestWriteThenReadFileNested3(t *testing.T) {
	ctx := context.Background()
	_ = os.Mkdir("files", 0755)

	wf, err := os.OpenFile("files/test7.parquet", os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0644)
	require.NoError(t, err, "creating file failed")

	w := NewFileWriter(wf, WithCompressionCodec(parquet.CompressionCodec_SNAPPY), WithCreator("parquet-go-unittest"))
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

	rf, err := os.Open("files/test7.parquet")
	require.NoError(t, err, "opening file failed")
	defer rf.Close()

	r, err := NewFileReader(rf)
	require.NoError(t, err, "creating file reader failed")
	require.NoError(t, r.readRowGroup(ctx))

	require.Equal(t, int64(len(data)), r.schemaReader.rowGroupNumRecords())
	for i := range data {
		d, err := r.schemaReader.getData()
		require.NoError(t, err)
		require.Equal(t, data[i], d)
	}
}

func TestWriteEmptyDict(t *testing.T) {
	ctx := context.Background()
	_ = os.Mkdir("files", 0755)

	wf, err := os.OpenFile("files/test8.parquet", os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0644)
	require.NoError(t, err, "creating file failed")

	w := NewFileWriter(wf, WithCompressionCodec(parquet.CompressionCodec_SNAPPY), WithCreator("parquet-go-unittest"))
	valueStore, err := NewByteArrayStore(parquet.Encoding_PLAIN, true, &ColumnParameters{})
	require.NoError(t, err, "failed to create valueStore")
	require.NoError(t, w.AddColumn("value", NewDataColumn(valueStore, parquet.FieldRepetitionType_OPTIONAL)))

	for i := 0; i < 1000; i++ {
		require.NoError(t, w.AddData(nil))
	}

	assert.NoError(t, w.Close(), "Close failed")

	require.NoError(t, wf.Close())

	rf, err := os.Open("files/test8.parquet")
	require.NoError(t, err, "opening file failed")
	defer rf.Close()

	r, err := NewFileReader(rf)
	require.NoError(t, err, "creating file reader failed")
	require.NoError(t, r.readRowGroup(ctx))

	require.Equal(t, int64(1000), r.schemaReader.rowGroupNumRecords())
	for i := 0; i < 1000; i++ {
		d, err := r.schemaReader.getData()
		require.NoError(t, err)
		require.Equal(t, map[string]interface{}{}, d)
	}
}

func TestWriteTimeData(t *testing.T) {
	_ = os.Mkdir("files", 0755)

	wf, err := os.OpenFile("files/test9.parquet", os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0644)
	require.NoError(t, err, "creating file failed")

	sd, err := parquetschema.ParseSchemaDefinition(`
		message foo {
			required int64 ts_nanos (TIMESTAMP(NANOS, true));
			required int64 ts_micros (TIMESTAMP(MICROS, true));
			required int64 ts_millis (TIMESTAMP(MILLIS, true));
			required int32 date (DATE);
			required int64 t_nanos (TIME(NANOS, false));
			required int64 t_micros (TIME(MICROS, false));
			required int32 t_millis (TIME(MILLIS, false));
			optional int32 t_alwaysnull (TIME(MILLIS, false));
		}
	`)
	require.NoError(t, err)

	w := NewFileWriter(wf, WithSchemaDefinition(sd), WithCompressionCodec(parquet.CompressionCodec_GZIP))
	testData := []time.Time{
		time.Date(2015, 5, 9, 14, 15, 45, 666777888, time.UTC),
		time.Date(1983, 10, 18, 11, 45, 16, 123456789, time.UTC),
	}

	for _, tt := range testData {
		require.NoError(t, w.AddData(map[string]interface{}{
			"ts_nanos":  tt.UnixNano(),
			"ts_micros": tt.UnixNano() / 1000,
			"ts_millis": tt.UnixNano() / 1000000,
			"date":      int32(tt.UnixNano() / (86400 * 1000000000)),
			"t_nanos":   int64((tt.Hour()*3600+tt.Minute()*60+tt.Second())*1000000000 + tt.Nanosecond()),
			"t_micros":  int64((tt.Hour()*3600+tt.Minute()*60+tt.Second())*1000000 + tt.Nanosecond()/1000),
			"t_millis":  int32((tt.Hour()*3600+tt.Minute()*60+tt.Second())*1000 + tt.Nanosecond()/1000000),
		}))
	}

	require.NoError(t, w.FlushRowGroup())
	require.NoError(t, w.Close())
	require.NoError(t, wf.Close())

	rf, err := os.Open("files/test9.parquet")
	require.NoError(t, err, "opening file failed")
	defer rf.Close()

	r, err := NewFileReader(rf)
	require.NoError(t, err, "creating file reader failed")

	require.NoError(t, r.PreLoad())

	rg := r.CurrentRowGroup()

	verificationData := []struct {
		pathInSchema  []string
		maxValue      []byte
		minValue      []byte
		nullCount     int64
		distinctCount int64
	}{
		{
			[]string{"ts_nanos"},
			[]byte{0x20, 0xa3, 0xc6, 0xc3, 0x7c, 0x93, 0xdc, 0x13},
			[]byte{0x15, 0xc5, 0x33, 0x1e, 0x40, 0x96, 0xa, 0x6},
			0,
			2,
		},
		{
			[]string{"ts_micros"},
			[]byte{0xd9, 0x32, 0xe0, 0xc7, 0xa6, 0x15, 0x5, 0x0},
			[]byte{0x40, 0xd, 0xc0, 0x1e, 0xed, 0x8b, 0x1, 0x0},
			0,
			2,
		},
		{
			[]string{"ts_millis"},
			[]byte{0x2, 0x29, 0x8, 0x39, 0x4d, 0x1, 0x0, 0x0},
			[]byte{0x5b, 0x39, 0x6c, 0x5b, 0x65, 0x0, 0x0, 0x0},
			0,
			2,
		},
		{
			[]string{"date"},
			[]byte{0xb4, 0x40, 0x0, 0x0},
			[]byte{0xae, 0x13, 0x0, 0x0},
			0,
			2,
		},
		{
			[]string{"t_nanos"},
			[]byte{0x20, 0xa3, 0x3a, 0xd8, 0xb2, 0x2e, 0x0, 0x0},
			[]byte{0x15, 0xc5, 0x81, 0x7d, 0x7c, 0x26, 0x0, 0x0},
			0,
			2,
		},
		{
			[]string{"t_micros"},
			[]byte{0xd9, 0xb2, 0x70, 0xf4, 0xb, 0x0, 0x0, 0x0},
			[]byte{0x40, 0xcd, 0x3c, 0xda, 0x9, 0x0, 0x0, 0x0},
			0,
			2,
		},
		{
			[]string{"t_millis"},
			[]byte{0x2, 0x79, 0xf, 0x3},
			[]byte{0x5b, 0xb1, 0x85, 0x2},
			0,
			2,
		},
		{
			[]string{"t_alwaysnull"},
			nil,
			nil,
			2,
			0,
		},
	}

	for idx, tt := range verificationData {
		assert.Equal(t, tt.pathInSchema, rg.Columns[idx].MetaData.PathInSchema, "%d. path in schema doesn't match", idx)
		assert.Equal(t, tt.maxValue, rg.Columns[idx].MetaData.Statistics.MaxValue, "%d. max value doesn't match", idx)
		assert.Equal(t, tt.minValue, rg.Columns[idx].MetaData.Statistics.MinValue, "%d. min value doesn't match", idx)
		assert.Equal(t, tt.nullCount, rg.Columns[idx].MetaData.Statistics.GetNullCount(), "%d. null count doesn't match", idx)
		assert.Equal(t, tt.distinctCount, rg.Columns[idx].MetaData.Statistics.GetDistinctCount(), "%d. distinct count doesn't match", idx)
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
	sd, err := parquetschema.ParseSchemaDefinition(sc)
	require.NoError(t, err)
	w := NewFileWriter(buf, WithSchemaDefinition(sd))

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

func TestWriteFileWithMarshallerThenReadWithUnmarshaller(t *testing.T) {
	sd, err := parquetschema.ParseSchemaDefinition(
		`message test_msg {
			required group baz (LIST) {
				repeated group list {
					required group element {
						required int64 quux;
					}
				}
			}
		}`)

	require.NoError(t, err, "parsing schema definition failed")

	buf := &bytes.Buffer{}
	hlWriter := NewFileWriter(
		buf,
		WithCompressionCodec(parquet.CompressionCodec_SNAPPY),
		WithCreator("floor-unittest"),
		WithSchemaDefinition(sd),
	)

	require.NoError(t, err, "creating new file writer failed")

	testData := map[string]interface{}{
		"baz": map[string]interface{}{
			"list": []map[string]interface{}{
				{
					"element": map[string]interface{}{
						"quux": int64(23),
					},
				},
				{
					"element": map[string]interface{}{
						"quux": int64(42),
					},
				},
			},
		},
	}

	require.NoError(t, hlWriter.AddData(testData), "writing object using marshaller failed")

	require.NoError(t, hlWriter.Close())

	hlReader, err := NewFileReader(bytes.NewReader(buf.Bytes()))
	require.NoError(t, err, "opening file failed")

	readData, err := hlReader.NextRow()
	require.NoError(t, err)
	require.Equal(t, testData, readData, "written and read data don't match")
}

func TestWriteWithFlushGroupMetaDataThenRead(t *testing.T) {
	sd, err := parquetschema.ParseSchemaDefinition(
		`message test_msg {
			required int64 foo;
			required group x {
				required int64 bar;
			}
		}`)

	require.NoError(t, err, "parsing schema definition failed")

	buf := &bytes.Buffer{}
	hlWriter := NewFileWriter(
		buf,
		WithCompressionCodec(parquet.CompressionCodec_SNAPPY),
		WithCreator("floor-unittest"),
		WithSchemaDefinition(sd),
		WithMetaData(map[string]string{"global": "metadata"}),
	)

	require.NoError(t, err, "creating new file writer failed")

	testData := map[string]interface{}{
		"foo": int64(23),
		"x": map[string]interface{}{
			"bar": int64(42),
		},
	}

	require.NoError(t, hlWriter.AddData(testData), "writing object using marshaller failed")

	require.NoError(t, hlWriter.Close(
		WithRowGroupMetaData(map[string]string{"a": "hello", "b": "world"}),
		WithRowGroupMetaDataForColumn("foo", map[string]string{"b": "friendo", "c": "!"}),
		WithRowGroupMetaDataForColumn("x.bar", map[string]string{"a": "goodbye"}),
	))

	hlReader, err := NewFileReader(bytes.NewReader(buf.Bytes()))
	require.NoError(t, err)

	require.Equal(t, map[string]string{"global": "metadata"}, hlReader.MetaData())

	require.NoError(t, hlReader.PreLoad())

	// the low-level way of inspecting column metadata:
	rg := hlReader.CurrentRowGroup()
	cols := rg.GetColumns()
	require.Equal(t, 2, len(cols))

	require.Equal(t, []string{"foo"}, cols[0].MetaData.PathInSchema)
	require.Equal(t, []*parquet.KeyValue{
		{Key: "a", Value: strPtr("hello")},
		{Key: "b", Value: strPtr("friendo")},
		{Key: "c", Value: strPtr("!")},
	}, cols[0].MetaData.KeyValueMetadata)

	require.Equal(t, []string{"x", "bar"}, cols[1].MetaData.PathInSchema)
	require.Equal(t, []*parquet.KeyValue{
		{Key: "a", Value: strPtr("goodbye")},
		{Key: "b", Value: strPtr("world")},
	}, cols[1].MetaData.KeyValueMetadata)

	// the high-level way of inspecting column metadata:
	fooMetaData, err := hlReader.ColumnMetaData("foo")
	require.NoError(t, err)
	require.Equal(t, map[string]string{"a": "hello", "b": "friendo", "c": "!"}, fooMetaData)

	xbarMetaData, err := hlReader.ColumnMetaData("x.bar")
	require.NoError(t, err)
	require.Equal(t, map[string]string{"a": "goodbye", "b": "world"}, xbarMetaData)

	_, err = hlReader.ColumnMetaData("does.not.exist")
	require.Error(t, err)
}

func TestReadWriteColumeEncodings(t *testing.T) {
	buf := &bytes.Buffer{}

	w := NewFileWriter(buf)

	s, err := NewBooleanStore(parquet.Encoding_RLE, &ColumnParameters{})
	require.NoError(t, err)
	require.NoError(t, w.AddColumn("a", NewDataColumn(s, parquet.FieldRepetitionType_REQUIRED)))

	s, err = NewBooleanStore(parquet.Encoding_PLAIN, &ColumnParameters{})
	require.NoError(t, err)
	require.NoError(t, w.AddColumn("b", NewDataColumn(s, parquet.FieldRepetitionType_REQUIRED)))

	s, err = NewByteArrayStore(parquet.Encoding_DELTA_LENGTH_BYTE_ARRAY, false, &ColumnParameters{})
	require.NoError(t, err)
	require.NoError(t, w.AddColumn("c", NewDataColumn(s, parquet.FieldRepetitionType_REQUIRED)))

	s, err = NewByteArrayStore(parquet.Encoding_DELTA_BYTE_ARRAY, false, &ColumnParameters{})
	require.NoError(t, err)
	require.NoError(t, w.AddColumn("d", NewDataColumn(s, parquet.FieldRepetitionType_REQUIRED)))

	s, err = NewFloatStore(parquet.Encoding_PLAIN, false, &ColumnParameters{})
	require.NoError(t, err)
	require.NoError(t, w.AddColumn("e", NewDataColumn(s, parquet.FieldRepetitionType_REQUIRED)))

	s, err = NewDoubleStore(parquet.Encoding_PLAIN, false, &ColumnParameters{})
	require.NoError(t, err)
	require.NoError(t, w.AddColumn("f", NewDataColumn(s, parquet.FieldRepetitionType_REQUIRED)))

	testData := map[string]interface{}{
		"a": true,
		"b": false,
		"c": []byte("hello"),
		"d": []byte("world"),
		"e": float32(23.0),
		"f": float64(42.0),
	}

	require.NoError(t, w.AddData(testData))

	require.NoError(t, w.Close())

	r, err := NewFileReader(bytes.NewReader(buf.Bytes()))
	require.NoError(t, err)

	data, err := r.NextRow()
	require.NoError(t, err)

	require.Equal(t, testData, data)

	_, err = r.NextRow()
	require.Equal(t, io.EOF, err)
}

func strPtr(s string) *string {
	return &s
}

func TestWriteThenReadFileUnsetOptional(t *testing.T) {
	sd, err := parquetschema.ParseSchemaDefinition(`
		message foo {
			optional group a (LIST) {
				repeated group list {
					optional group element {
						optional int64 b;
					}
				}
			}
		}`)
	require.NoError(t, err)

	var buf bytes.Buffer
	require.NoError(t, err)
	w := NewFileWriter(&buf, WithSchemaDefinition(sd))
	testData := map[string]interface{}{
		"a": map[string]interface{}{
			"list": []map[string]interface{}{
				{},
				{
					"element": map[string]interface{}{},
				},
				{
					"element": map[string]interface{}{
						"b": int64(2),
					},
				},
			},
		},
	}
	require.NoError(t, w.AddData(testData))
	require.NoError(t, w.Close())

	r, err := NewFileReader(bytes.NewReader(buf.Bytes()))
	require.NoError(t, err)

	data, err := r.NextRow()
	require.NoError(t, err)
	require.Equal(t, testData, data)

	_, err = r.NextRow()
	require.Equal(t, io.EOF, err)
}

func TestReadWriteFixedLenByteArrayEncodings(t *testing.T) {
	testData := []struct {
		name    string
		enc     parquet.Encoding
		useDict bool
		input   []byte
	}{
		{name: "delta_byte_array_with_dict", enc: parquet.Encoding_DELTA_BYTE_ARRAY, useDict: true, input: []byte{1, 3, 2, 14, 99, 42}},
		{name: "delta_byte_array_no_dict", enc: parquet.Encoding_DELTA_BYTE_ARRAY, useDict: false, input: []byte{7, 5, 254, 127, 42, 23}},
		{name: "plain_no_dict", enc: parquet.Encoding_PLAIN, useDict: false, input: []byte{9, 8, 7, 6, 5, 4}},
	}

	for _, tt := range testData {
		t.Run(tt.name, func(t *testing.T) {
			var buf bytes.Buffer
			wr := NewFileWriter(&buf)

			l := int32(len(tt.input))
			store, err := NewFixedByteArrayStore(tt.enc, tt.useDict, &ColumnParameters{TypeLength: &l})
			require.NoError(t, err)

			require.NoError(t, wr.AddColumn("value", NewDataColumn(store, parquet.FieldRepetitionType_REQUIRED)))

			inputRow := map[string]interface{}{"value": tt.input}

			require.NoError(t, wr.AddData(inputRow))

			require.NoError(t, wr.Close())

			rd, err := NewFileReader(bytes.NewReader(buf.Bytes()))
			require.NoError(t, err)

			outputRow, err := rd.NextRow()
			require.NoError(t, err)

			require.Equal(t, inputRow, outputRow)

			_, err = rd.NextRow()
			require.Error(t, err)
			require.True(t, errors.Is(err, io.EOF))
		})
	}
}

func TestReadWriteByteArrayEncodings(t *testing.T) {
	testData := []struct {
		name    string
		enc     parquet.Encoding
		useDict bool
		input   []byte
	}{
		{name: "delta_byte_array_with_dict", enc: parquet.Encoding_DELTA_BYTE_ARRAY, useDict: true, input: []byte{1, 3, 2, 14, 99, 42}},
		{name: "delta_byte_array_no_dict", enc: parquet.Encoding_DELTA_BYTE_ARRAY, useDict: false, input: []byte{7, 5, 254, 127, 42, 23}},
		{name: "delta_length_byte_array_with_dict", enc: parquet.Encoding_DELTA_LENGTH_BYTE_ARRAY, useDict: true, input: []byte{1, 5, 15, 25, 35, 75}},
		{name: "delta_length_byte_array_no_dict", enc: parquet.Encoding_DELTA_LENGTH_BYTE_ARRAY, useDict: false, input: []byte{75, 25, 5, 35, 15, 1}},
		{name: "plain_no_dict", enc: parquet.Encoding_PLAIN, useDict: false, input: []byte{9, 8, 7, 6, 5, 4}},
	}

	for _, tt := range testData {
		t.Run(tt.name, func(t *testing.T) {
			var buf bytes.Buffer
			wr := NewFileWriter(&buf)

			store, err := NewByteArrayStore(tt.enc, tt.useDict, &ColumnParameters{})
			require.NoError(t, err)

			require.NoError(t, wr.AddColumn("value", NewDataColumn(store, parquet.FieldRepetitionType_REQUIRED)))

			inputRow := map[string]interface{}{"value": tt.input}

			require.NoError(t, wr.AddData(inputRow))

			require.NoError(t, wr.Close())

			rd, err := NewFileReader(bytes.NewReader(buf.Bytes()))
			require.NoError(t, err)

			outputRow, err := rd.NextRow()
			require.NoError(t, err)

			require.Equal(t, inputRow, outputRow)

			_, err = rd.NextRow()
			require.Error(t, err)
			require.True(t, errors.Is(err, io.EOF))
		})
	}
}

func TestReadWriteInt64Encodings(t *testing.T) {
	testData := []struct {
		name    string
		enc     parquet.Encoding
		useDict bool
		input   int64
	}{
		{name: "plain_no_dict", enc: parquet.Encoding_PLAIN, useDict: false, input: 87743737636726},
		{name: "plain_with_dict", enc: parquet.Encoding_PLAIN, useDict: true, input: 42},
		{name: "delta_binary_packed", enc: parquet.Encoding_DELTA_BINARY_PACKED, useDict: false, input: 6363228832},
	}

	for _, tt := range testData {
		t.Run(tt.name, func(t *testing.T) {
			var buf bytes.Buffer

			wr := NewFileWriter(&buf)

			bas, err := NewInt64Store(tt.enc, tt.useDict, &ColumnParameters{})
			require.NoError(t, err)

			col := NewDataColumn(bas, parquet.FieldRepetitionType_REQUIRED)
			require.NoError(t, wr.AddColumn("number", col))

			inputRow := map[string]interface{}{
				"number": tt.input,
			}

			require.NoError(t, wr.AddData(inputRow))

			require.NoError(t, wr.Close())

			rd, err := NewFileReader(bytes.NewReader(buf.Bytes()))
			if err != nil {
				t.Fatal(err)
			}

			outputRow, err := rd.NextRow()
			require.NoError(t, err)

			require.Equal(t, inputRow, outputRow)

			_, err = rd.NextRow()
			require.True(t, errors.Is(err, io.EOF))
		})
	}
}

func TestReadWriteInt32Encodings(t *testing.T) {
	testData := []struct {
		name    string
		enc     parquet.Encoding
		useDict bool
		input   int32
	}{
		{name: "plain_no_dict", enc: parquet.Encoding_PLAIN, useDict: false, input: 3628282},
		{name: "plain_with_dict", enc: parquet.Encoding_PLAIN, useDict: true, input: 23},
		{name: "delta_binary_packed", enc: parquet.Encoding_DELTA_BINARY_PACKED, useDict: false, input: 9361082},
	}

	for _, tt := range testData {
		t.Run(tt.name, func(t *testing.T) {
			var buf bytes.Buffer

			wr := NewFileWriter(&buf)

			bas, err := NewInt32Store(tt.enc, tt.useDict, &ColumnParameters{})
			require.NoError(t, err)

			col := NewDataColumn(bas, parquet.FieldRepetitionType_REQUIRED)
			require.NoError(t, wr.AddColumn("number", col))

			inputRow := map[string]interface{}{
				"number": tt.input,
			}

			require.NoError(t, wr.AddData(inputRow))

			require.NoError(t, wr.Close())

			rd, err := NewFileReader(bytes.NewReader(buf.Bytes()))
			if err != nil {
				t.Fatal(err)
			}

			outputRow, err := rd.NextRow()
			require.NoError(t, err)

			require.Equal(t, inputRow, outputRow)

			_, err = rd.NextRow()
			require.True(t, errors.Is(err, io.EOF))
		})
	}
}

func TestReadWriteInt96Encodings(t *testing.T) {
	testData := []struct {
		name    string
		enc     parquet.Encoding
		useDict bool
		input   [12]byte
	}{
		{name: "plain_no_dict", enc: parquet.Encoding_PLAIN, useDict: false, input: TimeToInt96(time.Date(2020, 3, 16, 14, 30, 0, 0, time.UTC))},
		{name: "plain_with_dict", enc: parquet.Encoding_PLAIN, useDict: true, input: TimeToInt96(time.Now())},
	}

	for _, tt := range testData {
		t.Run(tt.name, func(t *testing.T) {
			var buf bytes.Buffer

			wr := NewFileWriter(&buf)

			bas, err := NewInt96Store(tt.enc, tt.useDict, &ColumnParameters{})
			require.NoError(t, err)

			col := NewDataColumn(bas, parquet.FieldRepetitionType_REQUIRED)
			require.NoError(t, wr.AddColumn("ts", col))

			inputRow := map[string]interface{}{
				"ts": tt.input,
			}

			require.NoError(t, wr.AddData(inputRow))

			require.NoError(t, wr.Close())

			rd, err := NewFileReader(bytes.NewReader(buf.Bytes()))
			if err != nil {
				t.Fatal(err)
			}

			outputRow, err := rd.NextRow()
			require.NoError(t, err)

			require.Equal(t, inputRow, outputRow)

			_, err = rd.NextRow()
			require.True(t, errors.Is(err, io.EOF))
		})
	}
}

func TestReadWriteFloatEncodings(t *testing.T) {
	testData := []struct {
		name    string
		enc     parquet.Encoding
		useDict bool
		input   float32
	}{
		{name: "plain_no_dict", enc: parquet.Encoding_PLAIN, useDict: false, input: 1.1111},
		{name: "plain_with_dict", enc: parquet.Encoding_PLAIN, useDict: true, input: 2.2222},
	}

	for _, tt := range testData {
		t.Run(tt.name, func(t *testing.T) {
			var buf bytes.Buffer

			wr := NewFileWriter(&buf)

			bas, err := NewFloatStore(tt.enc, tt.useDict, &ColumnParameters{})
			require.NoError(t, err)

			col := NewDataColumn(bas, parquet.FieldRepetitionType_REQUIRED)
			require.NoError(t, wr.AddColumn("number", col))

			inputRow := map[string]interface{}{
				"number": tt.input,
			}

			require.NoError(t, wr.AddData(inputRow))

			require.NoError(t, wr.Close())

			rd, err := NewFileReader(bytes.NewReader(buf.Bytes()))
			if err != nil {
				t.Fatal(err)
			}

			outputRow, err := rd.NextRow()
			require.NoError(t, err)

			require.Equal(t, inputRow, outputRow)

			_, err = rd.NextRow()
			require.True(t, errors.Is(err, io.EOF))
		})
	}
}

func TestReadWriteDoubleEncodings(t *testing.T) {
	testData := []struct {
		name    string
		enc     parquet.Encoding
		useDict bool
		input   float64
	}{
		{name: "plain_no_dict", enc: parquet.Encoding_PLAIN, useDict: false, input: 42.123456},
		{name: "plain_with_dict", enc: parquet.Encoding_PLAIN, useDict: true, input: 32.98765},
	}

	for _, tt := range testData {
		t.Run(tt.name, func(t *testing.T) {
			var buf bytes.Buffer

			wr := NewFileWriter(&buf)

			bas, err := NewDoubleStore(tt.enc, tt.useDict, &ColumnParameters{})
			require.NoError(t, err)

			col := NewDataColumn(bas, parquet.FieldRepetitionType_REQUIRED)
			require.NoError(t, wr.AddColumn("number", col))

			inputRow := map[string]interface{}{
				"number": tt.input,
			}

			require.NoError(t, wr.AddData(inputRow))

			require.NoError(t, wr.Close())

			rd, err := NewFileReader(bytes.NewReader(buf.Bytes()))
			if err != nil {
				t.Fatal(err)
			}

			outputRow, err := rd.NextRow()
			require.NoError(t, err)

			require.Equal(t, inputRow, outputRow)

			_, err = rd.NextRow()
			require.True(t, errors.Is(err, io.EOF))
		})
	}
}

func TestWriteThenReadMultiplePages(t *testing.T) {
	const mySchema = `message msg {
		required binary ts_str (STRING);
	}`

	sd, err := parquetschema.ParseSchemaDefinition(mySchema)
	require.NoError(t, err)

	testData := []struct {
		name    string
		options []FileWriterOption
	}{

		{
			name: "snappy",
			options: []FileWriterOption{
				WithSchemaDefinition(sd), WithCompressionCodec(parquet.CompressionCodec_SNAPPY),
			},
		},
		{
			name: "snappy_1kb_page",
			options: []FileWriterOption{
				WithSchemaDefinition(sd), WithCompressionCodec(parquet.CompressionCodec_SNAPPY), WithMaxPageSize(1 * 1024),
			},
		},
	}

	for _, tt := range testData {
		t.Run(tt.name, func(t *testing.T) {
			f := new(bytes.Buffer)

			fw := NewFileWriter(f, tt.options...)
			defer fw.Close()

			const numRows = 75

			records := []map[string]interface{}{}

			for i := 0; i < numRows; i++ {
				tsStr := time.Now().Add(time.Duration(1+rand.Int63n(300)) * time.Second).Format(time.RFC3339)
				rec := map[string]interface{}{"ts_str": []byte(tsStr)}
				records = append(records, rec)
				require.NoError(t, fw.AddData(rec))
			}

			require.NoError(t, fw.Close())

			r, err := NewFileReader(bytes.NewReader(f.Bytes()))
			require.NoError(t, err)

			rowCount := r.NumRows()
			require.Equal(t, int64(numRows), rowCount)

			for i := int64(0); i < rowCount; i++ {
				data, err := r.NextRow()
				require.NoError(t, err)
				require.Equal(t, records[i], data, "%d. records don't match", i)
				//fmt.Printf("in %d. %s\n", i, string(data["ts_str"].([]byte)))
			}
		})
	}
}

func TestReadWriteDoubleNaN(t *testing.T) {
	var buf bytes.Buffer

	wr := NewFileWriter(&buf)

	bas, err := NewDoubleStore(parquet.Encoding_PLAIN, true, &ColumnParameters{})
	require.NoError(t, err)

	col := NewDataColumn(bas, parquet.FieldRepetitionType_REQUIRED)
	require.NoError(t, wr.AddColumn("value", col))

	data := []float64{42.23, math.NaN(), math.NaN(), 23.42, math.Inf(1), math.Inf(-1), 1.111}

	for _, f := range data {
		require.NoError(t, wr.AddData(map[string]interface{}{
			"value": f,
		}))
	}

	require.NoError(t, wr.Close())

	rd, err := NewFileReader(bytes.NewReader(buf.Bytes()))
	if err != nil {
		t.Fatal(err)
	}

	for i := range data {
		outputRow, err := rd.NextRow()
		require.NoError(t, err)
		if math.IsNaN(data[i]) {
			require.True(t, math.IsNaN(outputRow["value"].(float64)))
		} else {
			require.Equal(t, data[i], outputRow["value"].(float64))
		}
	}

	_, err = rd.NextRow()
	require.True(t, errors.Is(err, io.EOF))
}

func TestReadWriteFloatNaN(t *testing.T) {
	var buf bytes.Buffer

	wr := NewFileWriter(&buf)

	bas, err := NewFloatStore(parquet.Encoding_PLAIN, true, &ColumnParameters{})
	require.NoError(t, err)

	col := NewDataColumn(bas, parquet.FieldRepetitionType_REQUIRED)
	require.NoError(t, wr.AddColumn("value", col))

	data := []float32{42.23, float32(math.NaN()), float32(math.NaN()), 23.42, float32(math.Inf(1)), float32(math.Inf(-1)), 1.111}

	for _, f := range data {
		require.NoError(t, wr.AddData(map[string]interface{}{
			"value": f,
		}))
	}

	require.NoError(t, wr.Close())

	rd, err := NewFileReader(bytes.NewReader(buf.Bytes()))
	if err != nil {
		t.Fatal(err)
	}

	for i := range data {
		outputRow, err := rd.NextRow()
		require.NoError(t, err)
		if math.IsNaN(float64(data[i])) {
			require.True(t, math.IsNaN(float64(outputRow["value"].(float32))))
		} else {
			require.Equal(t, data[i], outputRow["value"].(float32))
		}
	}

	_, err = rd.NextRow()
	require.True(t, errors.Is(err, io.EOF))
}

func TestWriteThenReadSetSchemaDefinition(t *testing.T) {
	var buf bytes.Buffer

	wr := NewFileWriter(&buf)

	sd, err := parquetschema.ParseSchemaDefinition(`message msg { required int64 foo; }`)
	require.NoError(t, err)

	require.NoError(t, wr.SetSchemaDefinition(sd))

	require.NoError(t, wr.AddData(map[string]interface{}{"foo": int64(23)}))

	require.NoError(t, wr.Close())

	require.Equal(t, sd.String(), wr.GetSchemaDefinition().String())

	require.Equal(t, 1, len(wr.Columns()))
	require.Equal(t, parquet.TypePtr(parquet.Type_INT64), wr.GetColumnByName("foo").Type())
	require.Nil(t, wr.GetColumnByName("bar"))
	require.Nil(t, wr.GetColumnByPath(ColumnPath{"bar"}))
	require.NotNil(t, wr.GetColumnByPath(ColumnPath{"foo"}))

	r, err := NewFileReader(bytes.NewReader(buf.Bytes()))
	require.NoError(t, err)

	sd2 := r.GetSchemaDefinition()

	require.Equal(t, sd.String(), sd2.String())

	row, err := r.NextRow()
	require.NoError(t, err)
	require.Equal(t, map[string]interface{}{"foo": int64(23)}, row)

	_, err = r.NextRow()
	require.True(t, errors.Is(err, io.EOF))
}

func TestRepeatedInt32(t *testing.T) {
	// this is here to somehow reproduce the issue discussed in https://github.com/fraugster/parquet-go/pull/8
	sd, err := parquetschema.ParseSchemaDefinition(`message msg {
		repeated int32 foo;
	}`)
	require.NoError(t, err)

	var buf bytes.Buffer
	fw := NewFileWriter(&buf, WithSchemaDefinition(sd))

	err = fw.AddData(map[string]interface{}{
		"foo": []int32{
			int32(23),
			int32(42),
			int32(9001),
		},
	})
	require.NoError(t, err)

	require.NoError(t, fw.Close())

	r, err := NewFileReader(bytes.NewReader(buf.Bytes()))
	require.NoError(t, err)

	row, err := r.NextRow()
	require.NoError(t, err)

	// here's a problem: we added nil, but got a []byte{}.
	require.Equal(t, []int32{
		int32(23),
		int32(42),
		int32(9001),
	}, row["foo"])
}
