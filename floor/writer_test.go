package floor

import (
	"fmt"
	"os"
	"reflect"
	"testing"
	"time"

	"github.com/davecgh/go-spew/spew"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	goparquet "github.com/fraugster/parquet-go"
	"github.com/fraugster/parquet-go/parquet"
)

func TestDecodeStruct(t *testing.T) {
	testData := []struct {
		Input          interface{}
		ExpectedOutput map[string]interface{}
		ExpectErr      bool
		Schema         string
	}{
		{
			Input:          struct{ Foo int16 }{Foo: 42},
			ExpectedOutput: map[string]interface{}{"foo": int32(42)},
			ExpectErr:      false,
			Schema:         `message test { required int32 foo; }`,
		},
		{
			Input:          struct{ Foo int }{Foo: 43},
			ExpectedOutput: map[string]interface{}{"foo": int32(43)},
			ExpectErr:      false,
			Schema:         `message test { required int32 foo; }`,
		},
		{
			Input:          struct{ Foo int8 }{Foo: 44},
			ExpectedOutput: map[string]interface{}{"foo": int32(44)},
			ExpectErr:      false,
			Schema:         `message test { required int32 foo; }`,
		},
		{
			Input:          struct{ Foo int32 }{Foo: 100000},
			ExpectedOutput: map[string]interface{}{"foo": int32(100000)},
			ExpectErr:      false,
			Schema:         `message test { required int32 foo; }`,
		},
		{
			Input:          struct{ Foo uint64 }{Foo: 1125899906842624},
			ExpectedOutput: map[string]interface{}{"foo": int64(1125899906842624)},
			ExpectErr:      false,
			Schema:         `message test { required int64 foo; }`,
		},
		{
			Input:          struct{ Foo uint }{Foo: 200000},
			ExpectedOutput: map[string]interface{}{"foo": int32(200000)},
			ExpectErr:      false,
			Schema:         `message test { required int32 foo; }`,
		},
		{
			Input:          struct{ Foo float32 }{Foo: 42.5},
			ExpectedOutput: map[string]interface{}{"foo": float32(42.5)},
			ExpectErr:      false,
			Schema:         `message test { required float foo; }`,
		},
		{
			Input:          struct{ Foo float64 }{Foo: 23.5},
			ExpectedOutput: map[string]interface{}{"foo": float64(23.5)},
			ExpectErr:      false,
			Schema:         `message test { required double foo; }`,
		},
		{
			Input:          struct{ Foo byte }{Foo: 1},
			ExpectedOutput: map[string]interface{}{"foo": int32(1)},
			ExpectErr:      false,
			Schema:         `message test { required int32 foo; }`,
		},
		{
			Input:          struct{ Foo string }{Foo: "bar"},
			ExpectedOutput: map[string]interface{}{"foo": "bar"},
			ExpectErr:      false,
			Schema:         `message test { required binary foo (STRING); }`,
		},
		{
			Input:          struct{ Foo *string }{Foo: new(string)},
			ExpectedOutput: map[string]interface{}{"foo": ""},
			ExpectErr:      false,
			Schema:         `message test { optional binary foo (STRING); }`,
		},
		{
			Input:          struct{ Foo *string }{},
			ExpectedOutput: map[string]interface{}{},
			ExpectErr:      false,
			Schema:         `message test { optional binary foo (STRING); }`,
		},
		{
			Input:          int(23),
			ExpectedOutput: nil,
			ExpectErr:      true,
			Schema:         `message test { }`,
		},
		{
			Input: struct {
				Foo struct {
					Bar int64
				}
				Baz  uint32
				Quux *bool
				Blub bool
			}{},
			ExpectedOutput: map[string]interface{}{"foo": map[string]interface{}{"bar": int64(0)}, "baz": int64(0), "blub": false},
			ExpectErr:      false,
			Schema:         `message test { required group foo { required int64 bar; } required int64 baz; optional boolean quux; required boolean blub; }`,
		},
		{
			Input: struct {
				Foo []bool
			}{
				Foo: []bool{false, true, false},
			},
			ExpectedOutput: map[string]interface{}{
				"foo": map[string]interface{}{
					"list": []map[string]interface{}{
						map[string]interface{}{"element": false},
						map[string]interface{}{"element": true},
						map[string]interface{}{"element": false},
					},
				},
			},
			ExpectErr: false,
			Schema: `message test {
				required group foo (LIST) {
					repeated group list {
						required boolean element;
					}
				}
			}`,
		},
		{
			Input: struct {
				Foo [5]uint16
			}{
				Foo: [5]uint16{1, 1, 2, 3, 5},
			},
			ExpectedOutput: map[string]interface{}{
				"foo": map[string]interface{}{
					"list": []map[string]interface{}{
						map[string]interface{}{"element": int32(1)},
						map[string]interface{}{"element": int32(1)},
						map[string]interface{}{"element": int32(2)},
						map[string]interface{}{"element": int32(3)},
						map[string]interface{}{"element": int32(5)},
					},
				},
			},
			ExpectErr: false,
			Schema: `message test {
				required group foo (LIST) {
					repeated group list {
						required int32 element;
					}
				}
			}`,
		},
		{
			Input: struct {
				Foo map[string]int64
			}{
				Foo: map[string]int64{
					"hello": int64(23),
				},
			},
			ExpectedOutput: map[string]interface{}{
				"foo": map[string]interface{}{
					"key_value": []map[string]interface{}{
						map[string]interface{}{"key": "hello", "value": int64(23)},
					},
				},
			},
			ExpectErr: false,
			Schema: `message test {
				required group foo (MAP) {
					repeated group key_value {
						required binary key (STRING);
						required int64 value;
					}
				}
			}`,
		},
		{
			Input: struct {
				C chan int
			}{},
			ExpectedOutput: nil,
			ExpectErr:      true,
			Schema:         `message foo { }`,
		},
		{
			Input: struct {
				Foo struct {
					C chan int
				}
			}{},
			ExpectedOutput: nil,
			ExpectErr:      true,
			Schema:         `message foo { required group foo { } }`,
		},
		{
			Input: struct {
				Foo []chan int
			}{Foo: []chan int{make(chan int)}},
			ExpectedOutput: nil,
			ExpectErr:      true,
			Schema:         `message foo { required group foo (LIST) { repeated group list { required int32 element; } } }`,
		},
		{
			Input: &struct {
				Bla int
			}{Bla: 616},
			ExpectedOutput: map[string]interface{}{"bla": int32(616)},
			ExpectErr:      false,
			Schema:         `message test { required int32 bla; }`,
		},
		{
			Input: (*struct {
				Bla int
			})(nil),
			ExpectedOutput: nil,
			ExpectErr:      true,
			Schema:         `message test { required int32 bla; }`,
		},
		{
			Input: struct {
				Date time.Time
			}{
				Date: time.Date(1970, 01, 10, 0, 0, 0, 0, time.UTC),
			},
			ExpectedOutput: map[string]interface{}{"date": int32(9)},
			ExpectErr:      false,
			Schema:         `message test { required int32 date (DATE); }`,
		},
		{
			Input: struct {
				Date time.Time
			}{
				Date: time.Date(1970, 01, 12, 23, 59, 59, 0, time.UTC),
			},
			ExpectedOutput: map[string]interface{}{"date": int32(11)},
			ExpectErr:      false,
			Schema:         `message test { required int32 date (DATE); }`,
		},
		{
			Input: struct {
				TS time.Time
			}{
				TS: time.Date(1970, 01, 01, 0, 0, 23, 0, time.UTC),
			},
			ExpectedOutput: map[string]interface{}{"ts": int64(23000)},
			ExpectErr:      false,
			Schema:         `message test { required int64 ts (TIMESTAMP(isAdjustedToUTC=false, unit=MILLIS)); }`,
		},
		{
			Input: struct {
				TS time.Time
			}{
				TS: time.Date(1970, 01, 01, 0, 0, 24, 0, time.UTC),
			},
			ExpectedOutput: map[string]interface{}{"ts": int64(24000000)},
			ExpectErr:      false,
			Schema:         `message test { required int64 ts (TIMESTAMP(isAdjustedToUTC=false, unit=MICROS)); }`,
		},
		{
			Input: struct {
				TS time.Time
			}{
				TS: time.Date(1970, 01, 01, 0, 0, 25, 2000, time.UTC),
			},
			ExpectedOutput: map[string]interface{}{"ts": int64(25000002000)},
			ExpectErr:      false,
			Schema:         `message test { required int64 ts (TIMESTAMP(isAdjustedToUTC=false, unit=NANOS)); }`,
		},
	}

	for idx, tt := range testData {
		sd, err := goparquet.ParseSchemaDefinition(tt.Schema)
		assert.NoError(t, err, "%d. parsing schema failed", idx)
		output, err := decodeStruct(reflect.ValueOf(tt.Input), sd)
		if tt.ExpectErr {
			assert.Error(t, err, "%d. expected error, but found none", idx)
		} else {
			assert.NoError(t, err, "%d. expected no error, but found one", idx)
			assert.Equal(t, tt.ExpectedOutput, output, "%d. output mismatch", idx)
		}
	}
}

func TestWriteFile(t *testing.T) {
	_ = os.Mkdir("files", 0755)

	sd, err := goparquet.ParseSchemaDefinition(
		`message test_msg {
			required int64 foo;
			optional binary bar (STRING);
			optional group baz (LIST) {
				repeated group list {
					required int32 element;
				}
			}
		}`)
	require.NoError(t, err, "parsing schema definition failed")

	t.Logf("schema definition: %s", spew.Sdump(sd))

	hlWriter, err := NewFileWriter(
		"files/test.parquet",
		goparquet.CompressionCodec(parquet.CompressionCodec_SNAPPY),
		goparquet.CreatedBy("floor-unittest"),
		goparquet.UseSchemaDefinition(sd),
	)
	require.NoError(t, err, "creating new file writer failed")

	data := []struct {
		Foo int64
		Bar *string
		Baz []int32
	}{
		{23, strPtr("hello!"), []int32{23}},
		{42, strPtr("world!"), []int32{1, 1, 2, 3, 5}},
		{500, nil, nil},
		{750, strPtr("empty"), nil},
		{1000, strPtr("bye!"), []int32{2, 3, 5, 7, 11}},
	}

	for idx, d := range data {
		ds, _ := decodeStruct(reflect.ValueOf(d), sd)
		t.Logf("%d. decodeStruct output = %s", idx, fmt.Sprintf("%#v", ds))
		require.NoError(t, hlWriter.Write(d), "%d. Write failed", idx)
	}

	require.NoError(t, hlWriter.Close())

	rf, err := os.Open("files/test.parquet")
	require.NoError(t, err)

	reader, err := goparquet.NewFileReader(rf)
	require.NoError(t, err)

	require.NoError(t, reader.ReadRowGroup())

	require.Equal(t, int64(len(data)), reader.NumRecords())

	expectedData := []map[string]interface{}{
		{
			"foo": int64(23),
			"bar": "hello!",
			"baz": map[string]interface{}{
				"list": []map[string]interface{}{
					map[string]interface{}{"element": int32(23)},
				},
			},
		},
		{
			"foo": int64(42),
			"bar": "world!",
			"baz": map[string]interface{}{
				"list": []map[string]interface{}{
					map[string]interface{}{"element": int32(1)},
					map[string]interface{}{"element": int32(1)},
					map[string]interface{}{"element": int32(2)},
					map[string]interface{}{"element": int32(3)},
					map[string]interface{}{"element": int32(5)},
				},
			},
		},
		{
			"foo": int64(500),
		},
		{
			"foo": int64(750),
			"bar": "empty",
		},
		{
			"foo": int64(1000),
			"bar": "bye!",
			"baz": map[string]interface{}{
				"list": []map[string]interface{}{
					map[string]interface{}{"element": int32(2)},
					map[string]interface{}{"element": int32(3)},
					map[string]interface{}{"element": int32(5)},
					map[string]interface{}{"element": int32(7)},
					map[string]interface{}{"element": int32(11)},
				},
			},
		},
	}

	for i := int64(0); i < reader.NumRecords(); i++ {
		data, err := reader.GetData()
		require.NoError(t, err, "%d. reading record failed")
		require.Equal(t, expectedData[i], data, "%d. data in parquet file differs from what's expected", i)
	}
}

func strPtr(s string) *string {
	return &s
}
