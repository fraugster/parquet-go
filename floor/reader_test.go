package floor

import (
	"os"
	"reflect"
	"testing"

	"github.com/davecgh/go-spew/spew"
	"github.com/stretchr/testify/require"
	goparquet "github.com/fraugster/parquet-go"
	"github.com/fraugster/parquet-go/parquet"
)

func TestReadFile(t *testing.T) {
	_ = os.Mkdir("files", 0755)

	sd, err := goparquet.ParseSchemaDefinition(
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
		goparquet.CompressionCodec(parquet.CompressionCodec_SNAPPY),
		goparquet.CreatedBy("floor-unittest"),
		goparquet.UseSchemaDefinition(sd),
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

	rf, err := os.Open("files/readtest.parquet")
	require.NoError(t, err)
	defer rf.Close()

	reader, err := goparquet.NewFileReader(rf)
	require.NoError(t, err)

	hlReader := NewReader(reader)

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
}

func TestReadWriteMap(t *testing.T) {
	_ = os.Mkdir("files", 0755)

	sd, err := goparquet.ParseSchemaDefinition(
		`message test_msg {
			required group foo (MAP) {
				repeated group key_value {
					required binary key (STRING);
					required int32 value;
				}
			}
		}`)
	require.NoError(t, err, "parsing schema definition failed")

	t.Logf("schema definition: %s", spew.Sdump(sd))

	hlWriter, err := NewFileWriter(
		"files/map.parquet",
		goparquet.CompressionCodec(parquet.CompressionCodec_SNAPPY),
		goparquet.CreatedBy("floor-unittest"),
		goparquet.UseSchemaDefinition(sd),
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

	rf, err := os.Open("files/map.parquet")
	require.NoError(t, err)
	defer rf.Close()

	reader, err := goparquet.NewFileReader(rf)
	require.NoError(t, err)

	hlReader := NewReader(reader)

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
}

func TestReadWriteSlice(t *testing.T) {
	_ = os.Mkdir("files", 0755)

	sd, err := goparquet.ParseSchemaDefinition(
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
		goparquet.CompressionCodec(parquet.CompressionCodec_SNAPPY),
		goparquet.CreatedBy("floor-unittest"),
		goparquet.UseSchemaDefinition(sd),
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

	rf, err := os.Open("files/list.parquet")
	require.NoError(t, err)
	defer rf.Close()

	reader, err := goparquet.NewFileReader(rf)
	require.NoError(t, err)

	hlReader := NewReader(reader)

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
}

func TestReadWriteArray(t *testing.T) {
	_ = os.Mkdir("files", 0755)

	sd, err := goparquet.ParseSchemaDefinition(
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
		goparquet.CompressionCodec(parquet.CompressionCodec_SNAPPY),
		goparquet.CreatedBy("floor-unittest"),
		goparquet.UseSchemaDefinition(sd),
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

	rf, err := os.Open("files/array.parquet")
	require.NoError(t, err)
	defer rf.Close()

	reader, err := goparquet.NewFileReader(rf)
	require.NoError(t, err)

	hlReader := NewReader(reader)

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
}

func TestFillValue(t *testing.T) {
	require.NoError(t, fillValue(reflect.New(reflect.TypeOf(true)).Elem(), false))
	require.Error(t, fillValue(reflect.New(reflect.TypeOf(true)).Elem(), 23))

	require.NoError(t, fillValue(reflect.New(reflect.TypeOf(int32(0))).Elem(), int64(23)))
	require.NoError(t, fillValue(reflect.New(reflect.TypeOf(int32(0))).Elem(), int32(23)))
	require.NoError(t, fillValue(reflect.New(reflect.TypeOf(int32(0))).Elem(), int16(23)))
	require.NoError(t, fillValue(reflect.New(reflect.TypeOf(int32(0))).Elem(), int8(23)))
	require.NoError(t, fillValue(reflect.New(reflect.TypeOf(int32(0))).Elem(), int(23)))
	require.Error(t, fillValue(reflect.New(reflect.TypeOf(int32(0))).Elem(), 3.5))

	require.NoError(t, fillValue(reflect.New(reflect.TypeOf(uint32(0))).Elem(), int64(42)))
	require.NoError(t, fillValue(reflect.New(reflect.TypeOf(uint32(0))).Elem(), int32(42)))
	require.NoError(t, fillValue(reflect.New(reflect.TypeOf(uint32(0))).Elem(), int16(42)))
	require.NoError(t, fillValue(reflect.New(reflect.TypeOf(uint32(0))).Elem(), int8(42)))
	require.NoError(t, fillValue(reflect.New(reflect.TypeOf(uint32(0))).Elem(), int(42)))
	require.Error(t, fillValue(reflect.New(reflect.TypeOf(uint32(0))).Elem(), "9001"))

	require.NoError(t, fillValue(reflect.New(reflect.TypeOf(float32(0.0))).Elem(), float64(23.5)))
	require.NoError(t, fillValue(reflect.New(reflect.TypeOf(float32(0.0))).Elem(), float32(23.5)))

	require.Error(t, fillValue(reflect.New(reflect.TypeOf(float32(0.0))).Elem(), false))

	require.NoError(t, fillValue(reflect.New(reflect.TypeOf("")).Elem(), "hello world!"))
	require.Error(t, fillValue(reflect.New(reflect.TypeOf("")).Elem(), int64(1000000)))
}
