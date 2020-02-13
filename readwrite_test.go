package goparquet

import (
	"bytes"
	"fmt"
	"io"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/fraugster/parquet-go/parquet"
	"github.com/fraugster/parquet-go/parquetschema"
)

func TestWriteThenReadFile(t *testing.T) {
	testFunc := func(opts ...FileWriterOption) {
		_ = os.Mkdir("files", 0755)

		wf, err := os.OpenFile("files/test1.parquet", os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0644)
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

		rf, err := os.Open("files/test1.parquet")
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
		for g := 0; g < r.RowGroupCount(); g++ {
			require.NoError(t, r.readRowGroup(), "Reading row group failed")
			for i := 0; i < int(r.rowGroupNumRecords()); i++ {
				data, err := r.getData()
				require.NoError(t, err)
				_, ok := data["foo"]
				require.True(t, ok)
			}
		}
	}

	testFunc(WithCompressionCodec(parquet.CompressionCodec_SNAPPY), WithCreator("parquet-go-unittest"))
	testFunc(WithCompressionCodec(parquet.CompressionCodec_SNAPPY), WithCreator("parquet-go-unittest"), WithDataPageV2())
}

func TestWriteThenReadFileRepeated(t *testing.T) {
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
	require.NoError(t, r.readRowGroup())

	require.Equal(t, int64(len(data)), r.rowGroupNumRecords())
	root := r.SchemaReader.(*schema).root
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

	wf, err := os.OpenFile("files/test4.parquet", os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0644)
	require.NoError(t, err, "creating file failed")

	w := NewFileWriter(wf, WithCompressionCodec(parquet.CompressionCodec_SNAPPY), WithCreator("parquet-go-unittest"))

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

	rf, err := os.Open("files/test4.parquet")
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
	require.NoError(t, r.readRowGroup())

	require.Equal(t, int64(1000), r.rowGroupNumRecords())
	for i := 0; i < 1000; i++ {
		d, err := r.getData()
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

func int64Ptr(i int64) *int64 {
	return &i
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

func TestWriteWithFlushGroupMeataDataThenRead(t *testing.T) {
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

	require.NoError(t, hlReader.PreLoad())

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
}

func strPtr(s string) *string {
	return &s
}
