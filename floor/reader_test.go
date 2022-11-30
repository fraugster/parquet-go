package floor

import (
	"fmt"
	"os"
	"reflect"
	"testing"
	"time"

	"github.com/davecgh/go-spew/spew"
	goparquet "github.com/fraugster/parquet-go"
	"github.com/fraugster/parquet-go/floor/interfaces"
	"github.com/fraugster/parquet-go/parquet"
	"github.com/fraugster/parquet-go/parquetschema"
	"github.com/stretchr/testify/require"
)

func TestNewReaderFailures(t *testing.T) {
	_, err := NewFileReader("file-does-not-exist.parquet")
	require.Error(t, err)

	_, err = NewFileReader("/dev/null")
	require.Error(t, err)
}

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

func TestReadWriteAthenaList(t *testing.T) {
	_ = os.Mkdir("files", 0755)

	sd, err := parquetschema.ParseSchemaDefinition(
		`message test_msg {
			required group emails (LIST) {
				repeated group bag {
					required binary array_element (STRING);
				}
			}
		}`)
	require.NoError(t, err, "parsing schema definition failed")

	t.Logf("schema definition: %s", spew.Sdump(sd))

	hlWriter, err := NewFileWriter(
		"files/athena_list.parquet",
		goparquet.WithCompressionCodec(parquet.CompressionCodec_SNAPPY),
		goparquet.WithCreator("floor-unittest"),
		goparquet.WithSchemaDefinition(sd),
	)
	require.NoError(t, err)

	testData := []string{"foo@example.com", "bar@example.com"}

	require.NoError(t, hlWriter.Write(&emailList{emails: testData}))

	require.NoError(t, hlWriter.Close())

	hlReader, err := NewFileReader("files/athena_list.parquet")
	require.NoError(t, err)

	var l emailList

	require.True(t, hlReader.Next())

	require.NoError(t, hlReader.Scan(&l))

	require.Equal(t, testData, l.emails)
}

type emailList struct {
	emails []string
}

func (l *emailList) MarshalParquet(obj interfaces.MarshalObject) error {
	list := obj.AddField("emails").List()
	for _, email := range l.emails {
		list.Add().SetByteArray([]byte(email))
	}
	return nil
}

func (l *emailList) UnmarshalParquet(obj interfaces.UnmarshalObject) error {
	list, err := obj.GetField("emails").List()
	if err != nil {
		return fmt.Errorf("couldn't get emails as list: %w", err)
	}

	for list.Next() {
		v, err := list.Value()
		if err != nil {
			return fmt.Errorf("couldn't get list value: %w", err)
		}
		vv, err := v.ByteArray()
		if err != nil {
			return fmt.Errorf("couldn't get list value as byte array: %w", err)
		}
		l.emails = append(l.emails, string(vv))
	}

	return nil
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
			ignored:   23,
		},
	}

	for _, tt := range testData {
		require.NoError(t, hlWriter.Write(tt))
	}
	require.NoError(t, hlWriter.Close())

	testData[0].ignored = 0

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
		required int96 tshive;
		required int64 tnano (TIME(NANOS, true));
		required int64 tmicro (TIME(MICROS, true));
		required int32 tmilli (TIME(MILLIS, true));
	}`)
	require.NoError(t, err)

	date := time.Unix(0, 0)
	require.NoError(t, um.fillValue(reflect.ValueOf(&date).Elem(), elem(int32(9)), sd.SubSchema("date")))
	require.Equal(t, date, time.Date(1970, 1, 10, 0, 0, 0, 0, time.UTC))

	ts := time.Unix(0, 0)
	require.NoError(t, um.fillValue(reflect.ValueOf(&ts).Elem(), elem(int64(42000000000)), sd.SubSchema("tsnano")))
	require.Equal(t, ts, time.Date(1970, 1, 1, 0, 0, 42, 0, time.UTC))

	require.NoError(t, um.fillValue(reflect.ValueOf(&ts).Elem(), elem(int64(1423000000)), sd.SubSchema("tsmicro")))
	require.Equal(t, ts, time.Date(1970, 1, 1, 0, 23, 43, 0, time.UTC))

	require.NoError(t, um.fillValue(reflect.ValueOf(&ts).Elem(), elem(int64(45299450)), sd.SubSchema("tsmilli")))
	require.Equal(t, ts, time.Date(1970, 1, 1, 12, 34, 59, 450000000, time.UTC))

	require.NoError(t, um.fillValue(reflect.ValueOf(&ts).Elem(), elem([12]byte{00, 0x60, 0xFD, 0x4B, 0x32, 0x29, 0x00, 0x00, 0x59, 0x68, 0x25, 0x00}), sd.SubSchema("tshive")))
	require.Equal(t, ts, time.Date(2000, 1, 1, 12, 34, 56, 0, time.UTC))

	var tt Time
	require.NoError(t, um.fillValue(reflect.ValueOf(&tt).Elem(), elem(int64(30000000010)), sd.SubSchema("tnano")))
	require.Equal(t, tt, MustTime(NewTime(0, 0, 30, 10)).UTC())

	require.NoError(t, um.fillValue(reflect.ValueOf(&tt).Elem(), elem(int64(210000020)), sd.SubSchema("tmicro")))
	require.Equal(t, tt, MustTime(NewTime(0, 3, 30, 20000)).UTC())

	require.NoError(t, um.fillValue(reflect.ValueOf(&tt).Elem(), elem(int32(14620200)), sd.SubSchema("tmilli")))
	require.Equal(t, tt, MustTime(NewTime(4, 3, 40, 200000000)).UTC())
}

func BenchmarkReadFile(b *testing.B) {
	_ = os.Mkdir("files", 0755)

	sd, err := parquetschema.ParseSchemaDefinition(
		`message test_msg {
			required int64 foo;
			optional binary bar (STRING);
			optional group baz {
				required int64 value;
			}
		}`)
	require.NoError(b, err, "parsing schema definition failed")

	hlWriter, err := NewFileWriter(
		"files/readtest.parquet",
		goparquet.WithCompressionCodec(parquet.CompressionCodec_SNAPPY),
		goparquet.WithCreator("floor-unittest"),
		goparquet.WithSchemaDefinition(sd),
	)
	require.NoError(b, err, "creating parquet file writer failed")

	type bazMsg struct {
		Value uint32
	}

	type testMsg struct {
		Foo int64
		Bar *string
		Baz *bazMsg
	}

	// Baz doesn't seem to get written correctly. when dumping the resulting file, baz.value is wrong.
	require.NoError(b, hlWriter.Write(testMsg{Foo: 1, Bar: strPtr("hello"), Baz: &bazMsg{Value: 9001}}))
	require.NoError(b, hlWriter.Close())

	hlReader, err := NewFileReader("files/readtest.parquet")
	require.NoError(b, err)
	defer func() {
		require.NoError(b, hlReader.Close())
	}()
	require.True(b, hlReader.Next())

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		var msg testMsg
		_ = hlReader.Scan(&msg)
	}
}

func TestCanReadMap(t *testing.T) {
	filename := os.Getenv("CDC_PARQUET_FILE")
	if filename == "" {
		filename = "/Users/mparsons/tmp/gen/part_1.parquet"
		//t.Skip("missing CDC_PARQUET_FILE, skipping")
	}
	type AV struct {
		B string `parquet:"b"`
		N string `parquet:"n"`
	}
	type CDC struct {
		TenantID                    string        `parquet:"tenantid"`
		RowID                       string        `parquet:"rowid"`
		TransactionID               string        `parquet:"transactionid"`
		Begin                       int64         `parquet:"begin"`
		End                         int64         `parquet:"end"`
		RecordType                  uint8         `parquet:"recordtype"`
		EventID                     string        `parquet:"eventid"`
		EventSource                 string        `parquet:"eventsource"`
		Operation                   uint8         `parquet:"operation"`
		SequenceNumber              string        `parquet:"sequencenumber"`
		ApproximateCreationDateTime [12]byte      `parquet:"approximatecreationdatetime"`
		Keys                        map[string]AV `parquet:"keys"`
		Old                         map[string]AV `parquet:"old"`
		New                         map[string]AV `parquet:"new"`
	}

	fr, err := NewFileReader(filename)
	require.NoError(t, err)

	for fr.Next() {
		var rec CDC
		require.NoError(t, fr.Scan(&rec))
	}
}
