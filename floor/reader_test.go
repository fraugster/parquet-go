package floor

import (
	"os"
	"reflect"
	"testing"
	"time"

	"github.com/fraugster/parquet-go/floor/interfaces"

	"github.com/davecgh/go-spew/spew"
	"github.com/stretchr/testify/require"
	goparquet "github.com/fraugster/parquet-go"
	"github.com/fraugster/parquet-go/parquet"
	"github.com/fraugster/parquet-go/parquetschema"
)

func TestReadFile(t *testing.T) {
	_ = os.Mkdir("files", 0755)

	sd, err := parquetschema.ParseSchemaDefinition(
		`message test_msg {
			required int64 foo;
			optional binary bar (STRING);
			optional group baz {
				required int64 value;
			}
		}`)
	require.NoError(t, err, "parsing schema definition failed")

	t.Logf("schema definition: %s", spew.Sdump(sd))

	hlWriter, err := NewFileWriter(
		"files/readtest.parquet",
		goparquet.WithCompressionCodec(parquet.CompressionCodec_SNAPPY),
		goparquet.WithCreator("floor-unittest"),
		goparquet.WithSchemaDefinition(sd),
	)
	require.NoError(t, err, "creating parquet file writer failed")

	type bazMsg struct {
		Value uint32
	}

	type testMsg struct {
		Foo int64
		Bar *string
		Baz *bazMsg
	}

	// Baz doesn't seem to get written correctly. when dumping the resulting file, baz.value is wrong.
	require.NoError(t, hlWriter.Write(testMsg{Foo: 1, Bar: strPtr("hello"), Baz: &bazMsg{Value: 9001}}))
	require.NoError(t, hlWriter.Write(&testMsg{Foo: 23}))
	require.NoError(t, hlWriter.Write(testMsg{Foo: 42, Bar: strPtr("world!")}))
	require.NoError(t, hlWriter.Close())

	hlReader, err := NewFileReader("files/readtest.parquet")
	require.NoError(t, err)

	count := 0

	var result []testMsg

	for hlReader.Next() {
		var msg testMsg

		require.Error(t, hlReader.Scan(int(1)), "%d. Scan into int unexpectedly succeeded", count)
		require.Error(t, hlReader.Scan(new(int)), "%d. Scan into *int unexpectedly succeeded", count)

		require.NoError(t, hlReader.Scan(&msg), "%d. Scan failed", count)
		t.Logf("%d. data = %#v", count, hlReader.data)

		result = append(result, msg)

		count++
	}

	require.NoError(t, hlReader.Err(), "hlReader returned error")
	require.False(t, hlReader.Next(), "hlReader returned true after it had returned false")

	t.Logf("count = %d", count)
	t.Logf("result = %s", spew.Sdump(result))

	require.NoError(t, hlReader.Err(), "hlReader returned an error")

	require.NoError(t, hlReader.Close())
}

func TestReadWriteMap(t *testing.T) {
	_ = os.Mkdir("files", 0755)

	sd, err := parquetschema.ParseSchemaDefinition(
		`message test_msg {
			required group foo (MAP) {
				repeated group key_value (MAP_KEY_VALUE) {
					required binary key (STRING);
					required int32 value;
				}
			}
		}`)
	require.NoError(t, err, "parsing schema definition failed")

	t.Logf("schema definition: %s", spew.Sdump(sd))

	hlWriter, err := NewFileWriter(
		"files/map.parquet",
		goparquet.WithCompressionCodec(parquet.CompressionCodec_SNAPPY),
		goparquet.WithCreator("floor-unittest"),
		goparquet.WithSchemaDefinition(sd),
	)
	require.NoError(t, err)

	type testMsg struct {
		Foo map[string]int32
	}

	testData := []testMsg{
		{Foo: map[string]int32{"foo": 23, "bar": 42, "baz": 9001}},
		{Foo: map[string]int32{"a": 61, "c": 63}},
	}

	for _, tt := range testData {
		require.NoError(t, hlWriter.Write(tt))
	}
	require.NoError(t, hlWriter.Close())

	hlReader, err := NewFileReader("files/map.parquet")
	require.NoError(t, err)

	count := 0

	var result []testMsg

	for hlReader.Next() {
		var msg testMsg

		require.NoError(t, hlReader.Scan(&msg), "%d. Scan failed", count)
		t.Logf("%d. data = %#v", count, hlReader.data)

		result = append(result, msg)

		count++
	}

	require.NoError(t, hlReader.Err(), "hlReader returned an error")
	t.Logf("count = %d", count)

	for idx, elem := range result {
		require.Equal(t, testData[idx], elem, "%d. read result doesn't match expected data", idx)
	}

	require.NoError(t, hlReader.Close())
}

func TestReadWriteSlice(t *testing.T) {
	_ = os.Mkdir("files", 0755)

	sd, err := parquetschema.ParseSchemaDefinition(
		`message test_msg {
			required group foo (LIST) {
				repeated group list {
					required binary element (STRING);
				}
			}
		}`)
	require.NoError(t, err, "parsing schema definition failed")

	t.Logf("schema definition: %s", spew.Sdump(sd))

	hlWriter, err := NewFileWriter(
		"files/list.parquet",
		goparquet.WithCompressionCodec(parquet.CompressionCodec_SNAPPY),
		goparquet.WithCreator("floor-unittest"),
		goparquet.WithSchemaDefinition(sd),
	)
	require.NoError(t, err)

	type testMsg struct {
		Foo []string
	}

	testData := []testMsg{
		{Foo: []string{"hello", "world!"}},
		{Foo: []string{"these", "are", "just", "my", "tokens"}},
		{Foo: []string{"bla"}},
	}

	for _, tt := range testData {
		require.NoError(t, hlWriter.Write(tt))
	}
	require.NoError(t, hlWriter.Close())

	hlReader, err := NewFileReader("files/list.parquet")
	require.NoError(t, err)

	count := 0

	var result []testMsg

	for hlReader.Next() {
		var msg testMsg

		require.NoError(t, hlReader.Scan(&msg), "%d. Scan failed", count)
		t.Logf("%d. data = %#v", count, hlReader.data)

		result = append(result, msg)

		count++
	}

	require.NoError(t, hlReader.Err(), "hlReader returned an error")
	t.Logf("count = %d", count)

	for idx, elem := range result {
		require.Equal(t, testData[idx], elem, "%d. read result doesn't match expected data")
	}

	require.NoError(t, hlReader.Close())
}

func TestReadWriteArray(t *testing.T) {
	_ = os.Mkdir("files", 0755)

	sd, err := parquetschema.ParseSchemaDefinition(
		`message test_msg {
			required group foo (LIST) {
				repeated group list {
					required binary element (STRING);
				}
			}
		}`)
	require.NoError(t, err, "parsing schema definition failed")

	t.Logf("schema definition: %s", spew.Sdump(sd))

	hlWriter, err := NewFileWriter(
		"files/array.parquet",
		goparquet.WithCompressionCodec(parquet.CompressionCodec_SNAPPY),
		goparquet.WithCreator("floor-unittest"),
		goparquet.WithSchemaDefinition(sd),
	)
	require.NoError(t, err)

	type testMsg struct {
		Foo [2]string
	}

	testData := []testMsg{
		{Foo: [2]string{"hello", "world!"}},
		{Foo: [2]string{"good morning", "vietnam!"}},
		{Foo: [2]string{"Berlin", "Zehlendorf"}},
	}

	for _, tt := range testData {
		require.NoError(t, hlWriter.Write(tt))
	}
	require.NoError(t, hlWriter.Close())

	hlReader, err := NewFileReader("files/array.parquet")
	require.NoError(t, err)

	count := 0

	var result []testMsg

	for hlReader.Next() {
		var msg testMsg

		require.NoError(t, hlReader.Scan(&msg), "%d. Scan failed", count)
		t.Logf("%d. data = %#v", count, hlReader.data)

		result = append(result, msg)

		count++
	}

	require.NoError(t, hlReader.Err(), "hlReader returned an error")

	t.Logf("count = %d", count)

	for idx, elem := range result {
		require.Equal(t, testData[idx], elem, "%d. read result doesn't match expected data")
	}

	require.NoError(t, hlReader.Close())
}

func TestReadWriteSpecialTypes(t *testing.T) {
	_ = os.Mkdir("files", 0755)

	sd, err := parquetschema.ParseSchemaDefinition(
		`message test_msg {
			required fixed_len_byte_array(16) theid (UUID);
			required binary clientstr (ENUM);
			required binary client (ENUM);
			required binary datastr (JSON);
			required binary data (JSON);
			optional int64 ignored;
		}`)
	require.NoError(t, err, "parsing schema definition failed")

	t.Logf("schema definition: %s", spew.Sdump(sd))

	hlWriter, err := NewFileWriter(
		"files/specialtypes.parquet",
		goparquet.WithCompressionCodec(parquet.CompressionCodec_SNAPPY),
		goparquet.WithCreator("floor-unittest"),
		goparquet.WithSchemaDefinition(sd),
	)
	require.NoError(t, err)

	type testMsg struct {
		TheID       [16]byte
		ClientStr   string
		Client      []byte
		DataStr     string
		Data        []byte
		ignored     int64 // ignored because it's private and therefore not settable.
		NotInSchema int64 // does not match up with anything in schema, therefore there shall be no attempt to fill it.
	}

	testData := []testMsg{
		{
			TheID:     [16]byte{0x0, 0x1, 0x2, 0x3, 0x4, 0x5, 0x6, 0x7, 0x8, 0x9, 0x0A, 0x0B, 0x0C, 0x0E, 0x0F, 0x10},
			ClientStr: "hello",
			Client:    []byte("world"),
			DataStr:   `{"foo":"bar","baz":23}`,
			Data:      []byte(`{"quux":{"foo":"bar"}}`),
		},
	}

	for _, tt := range testData {
		require.NoError(t, hlWriter.Write(tt))
	}
	require.NoError(t, hlWriter.Close())

	hlReader, err := NewFileReader("files/specialtypes.parquet")
	require.NoError(t, err)

	count := 0

	var result []testMsg

	for hlReader.Next() {
		var msg testMsg

		require.NoError(t, hlReader.Scan(&msg), "%d. Scan failed", count)
		t.Logf("%d. data = %#v", count, hlReader.data)

		result = append(result, msg)

		count++
	}

	require.NoError(t, hlReader.Err(), "hlReader returned an error")

	t.Logf("count = %d", count)

	for idx, elem := range result {
		require.Equal(t, testData[idx], elem, "%d. read result doesn't match expected data")
	}

	require.NoError(t, hlReader.Close())
}

func elem(data interface{}) interfaces.UnmarshalElement {
	return interfaces.NewUnmarshallElement(data)
}

func TestReflectUnmarshaller(t *testing.T) {
	obj1 := struct {
		Foo int64
	}{}

	sd, err := parquetschema.ParseSchemaDefinition(`message test { required int64 foo; }`)
	require.NoError(t, err)

	um := &reflectUnmarshaller{obj: obj1, schemaDef: sd}

	data := interfaces.NewUnmarshallObject(map[string]interface{}{"foo": int64(42)})

	err = um.UnmarshalParquet(data)
	require.EqualError(t, err, "you need to provide an object of type *struct { Foo int64 } to unmarshal into")

	i64 := int64(23)
	obj2 := &i64

	um.obj = obj2

	err = um.UnmarshalParquet(data)
	require.EqualError(t, err, "provided object of type *int64 is not a struct")

	um.obj = &obj1

	err = um.UnmarshalParquet(data)
	require.NoError(t, err)
}

func TestUnmarshallerFieldNameStructTag(t *testing.T) {
	obj1 := struct {
		Foo int64 `parquet:"bar"`
	}{}

	sd, err := parquetschema.ParseSchemaDefinition(`message test { required int64 bar; }`)
	require.NoError(t, err)

	um := &reflectUnmarshaller{obj: &obj1, schemaDef: sd}

	data := interfaces.NewUnmarshallObject(map[string]interface{}{"bar": int64(42)})

	err = um.UnmarshalParquet(data)
	require.NoError(t, err)
}

func TestFillValue(t *testing.T) {
	um := &reflectUnmarshaller{}

	require.NoError(t, um.fillValue(reflect.New(reflect.TypeOf(true)).Elem(), elem(false), nil))
	require.Error(t, um.fillValue(reflect.New(reflect.TypeOf(true)).Elem(), elem(23), nil))

	require.NoError(t, um.fillValue(reflect.New(reflect.TypeOf(int32(0))).Elem(), elem(int64(23)), nil))
	require.NoError(t, um.fillValue(reflect.New(reflect.TypeOf(int32(0))).Elem(), elem(int32(23)), nil))
	require.Error(t, um.fillValue(reflect.New(reflect.TypeOf(int32(0))).Elem(), elem(3.5), nil))

	require.NoError(t, um.fillValue(reflect.New(reflect.TypeOf(uint32(0))).Elem(), elem(int64(42)), nil))
	require.NoError(t, um.fillValue(reflect.New(reflect.TypeOf(uint32(0))).Elem(), elem(int32(42)), nil))
	require.Error(t, um.fillValue(reflect.New(reflect.TypeOf(uint32(0))).Elem(), elem("9001"), nil))

	require.NoError(t, um.fillValue(reflect.New(reflect.TypeOf(float32(0.0))).Elem(), elem(float64(23.5)), nil))
	require.NoError(t, um.fillValue(reflect.New(reflect.TypeOf(float32(0.0))).Elem(), elem(float32(23.5)), nil))

	require.Error(t, um.fillValue(reflect.New(reflect.TypeOf(float32(0.0))).Elem(), elem(false), nil))

	require.NoError(t, um.fillValue(reflect.New(reflect.TypeOf([]byte{})).Elem(), elem([]byte("hello world!")), nil))
	require.Error(t, um.fillValue(reflect.New(reflect.TypeOf([]byte{})).Elem(), elem(int64(1000000)), nil))

	sd, err := parquetschema.ParseSchemaDefinition(`message test {
		required int32 date (DATE);
		required int64 tsnano (TIMESTAMP(NANOS, true));
		required int64 tsmicro (TIMESTAMP(MICROS, true));
		required int64 tsmilli (TIMESTAMP(MILLIS, true));
		required int64 tnano (TIME(NANOS, true));
		required int64 tmicro (TIME(MICROS, true));
		required int32 tmilli (TIME(MILLIS, true));
	}`)
	require.NoError(t, err)

	date := time.Unix(0, 0)
	require.NoError(t, um.fillValue(reflect.ValueOf(&date).Elem(), elem(int32(9)), sd.SubSchema("date")))
	require.Equal(t, date, time.Date(1970, 01, 10, 0, 0, 0, 0, time.UTC))

	ts := time.Unix(0, 0)
	require.NoError(t, um.fillValue(reflect.ValueOf(&ts).Elem(), elem(int64(42000000000)), sd.SubSchema("tsnano")))
	require.Equal(t, ts, time.Date(1970, 01, 01, 0, 0, 42, 0, time.UTC))

	require.NoError(t, um.fillValue(reflect.ValueOf(&ts).Elem(), elem(int64(1423000000)), sd.SubSchema("tsmicro")))
	require.Equal(t, ts, time.Date(1970, 01, 01, 0, 23, 43, 0, time.UTC))

	require.NoError(t, um.fillValue(reflect.ValueOf(&ts).Elem(), elem(int64(45299450)), sd.SubSchema("tsmilli")))
	require.Equal(t, ts, time.Date(1970, 01, 01, 12, 34, 59, 450000000, time.UTC))

	var tt Time
	require.NoError(t, um.fillValue(reflect.ValueOf(&tt).Elem(), elem(int64(30000000010)), sd.SubSchema("tnano")))
	require.Equal(t, tt, MustTime(NewTime(0, 0, 30, 10)).UTC())

	require.NoError(t, um.fillValue(reflect.ValueOf(&tt).Elem(), elem(int64(210000020)), sd.SubSchema("tmicro")))
	require.Equal(t, tt, MustTime(NewTime(0, 3, 30, 20000)).UTC())

	require.NoError(t, um.fillValue(reflect.ValueOf(&tt).Elem(), elem(int32(14620200)), sd.SubSchema("tmilli")))
	require.Equal(t, tt, MustTime(NewTime(4, 3, 40, 200000000)).UTC())
}
