package floor

import (
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/davecgh/go-spew/spew"
	goparquet "github.com/fraugster/parquet-go"
	"github.com/fraugster/parquet-go/floor/interfaces"
	"github.com/fraugster/parquet-go/parquet"
	"github.com/fraugster/parquet-go/parquetschema"
	"github.com/stretchr/testify/require"
)

func TestDecodeStruct(t *testing.T) {
	testData := []struct {
		Input          any
		ExpectedOutput map[string]any
		ExpectErr      bool
		Schema         string
	}{
		{
			Input:          struct{ Foo int16 }{Foo: 42},
			ExpectedOutput: map[string]any{"foo": int32(42)},
			ExpectErr:      false,
			Schema:         `message test { required int32 foo; }`,
		},
		{
			Input:          struct{ Foo int }{Foo: 43},
			ExpectedOutput: map[string]any{"foo": int32(43)},
			ExpectErr:      false,
			Schema:         `message test { required int32 foo; }`,
		},
		{
			Input:          struct{ Foo int8 }{Foo: 44},
			ExpectedOutput: map[string]any{"foo": int32(44)},
			ExpectErr:      false,
			Schema:         `message test { required int32 foo; }`,
		},
		{
			Input:          struct{ Foo int32 }{Foo: 100000},
			ExpectedOutput: map[string]any{"foo": int32(100000)},
			ExpectErr:      false,
			Schema:         `message test { required int32 foo; }`,
		},
		{
			Input:          struct{ Foo uint64 }{Foo: 1125899906842624},
			ExpectedOutput: map[string]any{"foo": int64(1125899906842624)},
			ExpectErr:      false,
			Schema:         `message test { required int64 foo; }`,
		},
		{
			Input:          struct{ Foo uint }{Foo: 200000},
			ExpectedOutput: map[string]any{"foo": int32(200000)},
			ExpectErr:      false,
			Schema:         `message test { required int32 foo; }`,
		},
		{
			Input:          struct{ Foo float32 }{Foo: 42.5},
			ExpectedOutput: map[string]any{"foo": float32(42.5)},
			ExpectErr:      false,
			Schema:         `message test { required float foo; }`,
		},
		{
			Input:          struct{ Foo float64 }{Foo: 23.5},
			ExpectedOutput: map[string]any{"foo": float64(23.5)},
			ExpectErr:      false,
			Schema:         `message test { required double foo; }`,
		},
		{
			Input:          struct{ Foo byte }{Foo: 1},
			ExpectedOutput: map[string]any{"foo": int32(1)},
			ExpectErr:      false,
			Schema:         `message test { required int32 foo; }`,
		},
		{
			Input:          struct{ Foo string }{Foo: "bar"},
			ExpectedOutput: map[string]any{"foo": []byte("bar")},
			ExpectErr:      false,
			Schema:         `message test { required binary foo (STRING); }`,
		},
		{
			Input:          struct{ Foo *string }{Foo: new(string)},
			ExpectedOutput: map[string]any{"foo": []byte("")},
			ExpectErr:      false,
			Schema:         `message test { optional binary foo (STRING); }`,
		},
		{
			Input:          struct{ Foo *string }{},
			ExpectedOutput: map[string]any{},
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
				Quux *bool
				Baz  uint32
				Blub bool
			}{},
			ExpectedOutput: map[string]any{"foo": map[string]any{"bar": int64(0)}, "baz": int64(0), "blub": false},
			ExpectErr:      false,
			Schema:         `message test { required group foo { required int64 bar; } required int64 baz; optional boolean quux; required boolean blub; }`,
		},
		{
			Input: struct {
				Foo []bool
			}{
				Foo: []bool{false, true, false},
			},
			ExpectedOutput: map[string]any{
				"foo": map[string]any{
					"list": []map[string]any{
						{"element": false},
						{"element": true},
						{"element": false},
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
			ExpectedOutput: map[string]any{
				"foo": map[string]any{
					"list": []map[string]any{
						{"element": int32(1)},
						{"element": int32(1)},
						{"element": int32(2)},
						{"element": int32(3)},
						{"element": int32(5)},
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
			ExpectedOutput: map[string]any{
				"foo": map[string]any{
					"key_value": []map[string]any{
						{"key": []byte("hello"), "value": int64(23)},
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
			ExpectedOutput: map[string]any{},
			ExpectErr:      false,
			Schema:         `message foo { }`,
		},
		{
			Input: struct {
				Foo struct {
					C   chan int
					Bar int
				}
			}{},
			ExpectedOutput: map[string]any{"foo": map[string]any{"bar": int64(0)}},
			ExpectErr:      false,
			Schema:         `message foo { required group foo { optional int64 bar; } }`,
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
			ExpectedOutput: map[string]any{"bla": int32(616)},
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
				Date: time.Date(1970, 1, 10, 0, 0, 0, 0, time.UTC),
			},
			ExpectedOutput: map[string]any{"date": int32(9)},
			ExpectErr:      false,
			Schema:         `message test { required int32 date (DATE); }`,
		},
		{
			Input: struct {
				Date time.Time
			}{
				Date: time.Date(1970, 1, 12, 23, 59, 59, 0, time.UTC),
			},
			ExpectedOutput: map[string]any{"date": int32(11)},
			ExpectErr:      false,
			Schema:         `message test { required int32 date (DATE); }`,
		},
		{
			Input: struct {
				TS time.Time
			}{
				TS: time.Date(1970, 1, 1, 0, 0, 23, 0, time.UTC),
			},
			ExpectedOutput: map[string]any{"ts": int64(23000)},
			ExpectErr:      false,
			Schema:         `message test { required int64 ts (TIMESTAMP(MILLIS, false)); }`,
		},
		{
			Input: struct {
				TS time.Time
			}{
				TS: time.Date(1970, 1, 1, 0, 0, 24, 0, time.UTC),
			},
			ExpectedOutput: map[string]any{"ts": int64(24000000)},
			ExpectErr:      false,
			Schema:         `message test { required int64 ts (TIMESTAMP(MICROS, false)); }`,
		},
		{
			Input: struct {
				TS time.Time
			}{
				TS: time.Date(1970, 1, 1, 0, 0, 25, 2000, time.UTC),
			},
			ExpectedOutput: map[string]any{"ts": int64(25000002000)},
			ExpectErr:      false,
			Schema:         `message test { required int64 ts (TIMESTAMP(NANOS, false)); }`,
		},
		{
			Input: struct {
				Lunch Time
			}{
				Lunch: MustTime(NewTime(12, 30, 0, 0)),
			},
			ExpectedOutput: map[string]any{"lunch": int32(45000000)},
			ExpectErr:      false,
			Schema:         `message test { required int32 lunch (TIME(MILLIS, false)); }`,
		},
		{
			Input: struct {
				BeddyByes Time
			}{
				BeddyByes: MustTime(NewTime(20, 15, 30, 0)),
			},
			ExpectedOutput: map[string]any{"beddybyes": int64(72930000000)},
			ExpectErr:      false,
			Schema:         `message test { required int64 beddybyes (TIME(MICROS, false)); }`,
		},
		{
			Input: struct {
				WakeyWakey Time
			}{
				WakeyWakey: MustTime(NewTime(7, 5, 59, 0)),
			},
			ExpectedOutput: map[string]any{"wakeywakey": int64(25559000000000)},
			ExpectErr:      false,
			Schema:         `message test { required int64 wakeywakey (TIME(NANOS, false)); }`,
		},
		{
			Input: struct {
				Foo   string
				Times []any
			}{
				Foo:   "bar",
				Times: []any{"2021-10-29T20:06:47.960577000Z", 1635542684, 1635542811912, 1635542811912010, 1635542854925031000},
			},
			ExpectedOutput: map[string]any{
				"foo": []byte("bar"),
				"times": map[string]any{
					"list": []map[string]any{
						{"element": goparquet.TimeToInt96(time.Date(2021, 10, 29, 20, 06, 47, 960577000, time.UTC))},
						{"element": goparquet.TimeToInt96(time.Date(2021, 10, 29, 21, 24, 44, 0, time.UTC))},
						{"element": goparquet.TimeToInt96(time.Date(2021, 10, 29, 21, 26, 51, 912000000, time.UTC))},
						{"element": goparquet.TimeToInt96(time.Date(2021, 10, 29, 21, 26, 51, 912010000, time.UTC))},
						{"element": goparquet.TimeToInt96(time.Date(2021, 10, 29, 21, 27, 34, 925031000, time.UTC))},
					},
				},
			},
			ExpectErr: false,
			Schema: `message test {
				optional binary foo (STRING);
				optional group times (LIST) {
					repeated group list {
						required int96 element;
					}
				}
			}`,
		},
		{
			Input:          map[string]any{"foo": "bar"},
			ExpectedOutput: map[string]any{"foo": []byte("bar")},
			ExpectErr:      false,
			Schema:         `message test { optional binary foo (STRING); }`,
		},
		{
			Input: map[string]any{"foo": "bar", "data": map[string]any{"foo": "bar"}},
			ExpectedOutput: map[string]any{
				"foo": []byte("bar"),
				"data": map[string]any{
					"key_value": []map[string]any{
						{"key": []byte("foo"), "value": []byte("bar")},
					},
				}},
			ExpectErr: false,
			Schema: `message test {
				optional binary foo (STRING);
				required group data (MAP) {
					repeated group key_value {
						required binary key (STRING);
						optional binary value (STRING);
					}
				}
			}`,
		},
	}

	for idx, tt := range testData {
		t.Run(fmt.Sprintf("test_%d", idx), func(t *testing.T) {
			sd, err := parquetschema.ParseSchemaDefinition(tt.Schema)
			require.NoError(t, err, "%d. parsing schema failed", idx)
			obj := interfaces.NewMarshallObject(nil)
			m := &reflectMarshaller{obj: tt.Input, schemaDef: sd}
			err = m.MarshalParquet(obj)
			if tt.ExpectErr {
				require.Error(t, err, "%d. expected error, but found none", idx)
			} else {
				require.NoError(t, err, "%d. expected no error, but found one", idx)
				require.Equal(t, tt.ExpectedOutput, obj.GetData(), "%d. output mismatch; schema = %s", idx, tt.Schema)
			}
		})
	}
}

func TestWriteFile(t *testing.T) {
	_ = os.Mkdir("files", 0755)

	sd, err := parquetschema.ParseSchemaDefinition(
		`message test_msg {
			required int64 foo;
			optional binary bar (STRING);
			optional group baz (LIST) {
				repeated group list {
					required int32 element;
				}
			}
			optional int64 ts (TIMESTAMP(NANOS, false));
			optional int64 time (TIME(NANOS, false));
		}`)
	require.NoError(t, err, "parsing schema definition failed")

	t.Logf("schema definition: %s", spew.Sdump(sd))

	hlWriter, err := NewFileWriter(
		"files/test.parquet",
		goparquet.WithCompressionCodec(parquet.CompressionCodec_SNAPPY),
		goparquet.WithCreator("floor-unittest"),
		goparquet.WithSchemaDefinition(sd),
	)
	require.NoError(t, err, "creating new file writer failed")

	data := []struct {
		Foo  int64
		Bar  *string
		Baz  []int32
		Time *Time
	}{
		{23, strPtr("hello!"), []int32{23}, nil},
		{42, strPtr("world!"), []int32{1, 1, 2, 3, 5}, nil},
		{500, nil, nil, nil},
		{750, strPtr("empty"), nil, nil},
		{1000, strPtr("bye!"), []int32{2, 3, 5, 7, 11}, timePtr(MustTime(NewTime(16, 20, 0, 0)))},
	}

	for idx, d := range data {
		require.NoError(t, hlWriter.Write(d), "%d. Write failed", idx)
	}

	require.NoError(t, hlWriter.Close())

	rf, err := os.Open("files/test.parquet")
	require.NoError(t, err)

	reader, err := goparquet.NewFileReader(rf)
	require.NoError(t, err)

	n, err := reader.RowGroupNumRows()
	require.NoError(t, err)
	require.Equal(t, int64(len(data)), n)

	expectedData := []map[string]any{
		{
			"foo": int64(23),
			"bar": []byte("hello!"),
			"baz": map[string]any{
				"list": []map[string]any{
					{"element": int32(23)},
				},
			},
		},
		{
			"foo": int64(42),
			"bar": []byte("world!"),
			"baz": map[string]any{
				"list": []map[string]any{
					{"element": int32(1)},
					{"element": int32(1)},
					{"element": int32(2)},
					{"element": int32(3)},
					{"element": int32(5)},
				},
			},
		},
		{
			"foo": int64(500),
		},
		{
			"foo": int64(750),
			"bar": []byte("empty"),
		},
		{
			"foo": int64(1000),
			"bar": []byte("bye!"),
			"baz": map[string]any{
				"list": []map[string]any{
					{"element": int32(2)},
					{"element": int32(3)},
					{"element": int32(5)},
					{"element": int32(7)},
					{"element": int32(11)},
				},
			},
			"time": int64(58800000000000),
		},
	}

	n, err = reader.RowGroupNumRows()
	require.NoError(t, err)

	for i := int64(0); i < n; i++ {
		data, err := reader.NextRow()
		require.NoError(t, err, "%d. reading record failed")
		require.Equal(t, expectedData[i], data, "%d. data in parquet file differs from what's expected", i)
	}
}

func timePtr(t Time) *Time {
	return &t
}

func strPtr(s string) *string {
	return &s
}

func TestWriteReadByteArrays(t *testing.T) {
	_ = os.Mkdir("files", 0755)

	sd, err := parquetschema.ParseSchemaDefinition(
		`message test_msg {
			required fixed_len_byte_array(4) foo;
			optional fixed_len_byte_array(4) bar;
			required binary baz;
			optional binary quux;
		}`)
	require.NoError(t, err, "parsing schema definition failed")

	t.Logf("schema definition: %s", spew.Sdump(sd))

	hlWriter, err := NewFileWriter(
		"files/bytearrays.parquet",
		goparquet.WithCompressionCodec(parquet.CompressionCodec_SNAPPY),
		goparquet.WithCreator("floor-unittest"),
		goparquet.WithSchemaDefinition(sd),
	)
	require.NoError(t, err, "creating new file writer failed")

	type testData struct {
		Foo  [4]byte
		Bar  []byte
		Baz  []byte
		Quux []byte
	}

	data := []testData{
		{Foo: [4]byte{0, 1, 2, 3}, Bar: []byte{4, 5, 6, 7}, Baz: []byte{99}, Quux: []byte{100, 101}},
		{Foo: [4]byte{8, 9, 10, 11}, Baz: []byte("hello world!")},
		{Foo: [4]byte{12, 13, 14, 15}, Bar: []byte{16, 17, 18, 19}, Baz: []byte{155, 156, 157, 158, 159, 160}, Quux: []byte{180, 181, 182, 183}},
	}

	for idx, record := range data {
		require.NoError(t, hlWriter.Write(record), "%d. writing record failed", idx)
	}
	require.NoError(t, hlWriter.Close())

	hlReader, err := NewFileReader("files/bytearrays.parquet")
	require.NoError(t, err, "creating new file reader failed")

	var readData []testData

	for hlReader.Next() {
		var record testData
		require.NoError(t, hlReader.Scan(&record))
		readData = append(readData, record)
	}

	require.Equal(t, data, readData, "data written and read back doesn't match")
}

func TestWriteFileWithMarshallerThenReadWithUnmarshaller(t *testing.T) {
	_ = os.Mkdir("files", 0755)

	sd, err := parquetschema.ParseSchemaDefinition(
		`message test_msg {
			required binary foo (STRING);
			required int64 bar;
			required group baz (LIST) {
				repeated group list {
					required group element {
						required int64 quux;
					}
				}
			}
		}`)
	require.NoError(t, err, "parsing schema definition failed")

	t.Logf("schema definition: %s", spew.Sdump(sd))

	hlWriter, err := NewFileWriter(
		"files/marshaller.parquet",
		goparquet.WithCompressionCodec(parquet.CompressionCodec_SNAPPY),
		goparquet.WithCreator("floor-unittest"),
		goparquet.WithSchemaDefinition(sd),
	)
	require.NoError(t, err, "creating new file writer failed")

	testData := &marshTestRecord{foo: "hello world!", bar: 1234567, baz: []marshTestGroup{{quux: 23}, {quux: 42}}}
	require.NoError(t, hlWriter.Write(testData), "writing object using marshaller failed")

	require.NoError(t, hlWriter.Close())

	hlReader, err := NewFileReader("files/marshaller.parquet")
	require.NoError(t, err, "opening file failed")

	require.True(t, hlReader.Next())

	readData := &marshTestRecord{}
	require.NoError(t, hlReader.Scan(readData))

	require.Equal(t, testData, readData, "written and read data don't match")
	require.NoError(t, hlReader.Close())
}

func BenchmarkWriteFile(b *testing.B) {
	_ = os.Mkdir("files", 0755)

	sd, err := parquetschema.ParseSchemaDefinition(
		`message test_msg {
			required int64 foo;
			optional binary bar (STRING);
			optional group baz (LIST) {
				repeated group list {
					required int32 element;
				}
			}
			optional int64 ts (TIMESTAMP(NANOS, false));
			optional int64 time (TIME(NANOS, false));
		}`)
	require.NoError(b, err, "parsing schema definition failed")

	hlWriter, err := NewFileWriter(
		"files/test.parquet",
		goparquet.WithCompressionCodec(parquet.CompressionCodec_SNAPPY),
		goparquet.WithCreator("floor-unittest"),
		goparquet.WithSchemaDefinition(sd),
	)
	require.NoError(b, err, "creating new file writer failed")
	defer func() {
		require.NoError(b, hlWriter.Close())
	}()

	data := struct {
		Foo  int64
		Bar  *string
		Baz  []int32
		Time *Time
	}{
		42, strPtr("world!"), []int32{1, 1, 2, 3, 5}, nil,
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = hlWriter.Write(data)
	}
}

type marshTestRecord struct {
	foo string
	bar int64
	baz []marshTestGroup
}

type marshTestGroup struct {
	quux int64
}

func (r *marshTestRecord) MarshalParquet(obj interfaces.MarshalObject) error {
	obj.AddField("foo").SetByteArray([]byte(r.foo))
	obj.AddField("bar").SetInt64(r.bar)
	list := obj.AddField("baz").List()
	for _, b := range r.baz {
		grp := list.Add().Group()
		grp.AddField("quux").SetInt64(b.quux)
	}

	return nil
}

func (r *marshTestRecord) UnmarshalParquet(obj interfaces.UnmarshalObject) error {
	foo := obj.GetField("foo")
	if err := foo.Error(); err != nil {
		return err
	}

	fooValue, err := foo.ByteArray()
	if err != nil {
		return err
	}

	r.foo = string(fooValue)

	bar := obj.GetField("bar")
	if err = bar.Error(); err != nil {
		return err
	}

	barValue, err := bar.Int64()
	if err != nil {
		return err
	}

	r.bar = barValue

	bazList, err := obj.GetField("baz").List()
	if err != nil {
		return err
	}

	for bazList.Next() {
		v, err := bazList.Value()
		if err != nil {
			return err
		}

		grp, err := v.Group()
		if err != nil {
			return err
		}

		quux, err := grp.GetField("quux").Int64()
		if err != nil {
			return err
		}

		r.baz = append(r.baz, marshTestGroup{quux: quux})
	}

	return nil
}

type testMsg struct {
	ID     int64
	Foobar []string
}

func (m *testMsg) MarshalParquet(obj interfaces.MarshalObject) error {
	obj.AddField("id").SetInt64(m.ID)
	list := obj.AddField("foobar").List()
	for _, elem := range m.Foobar {
		list.Add().SetByteArray([]byte(elem))
	}
	return nil
}

func (m *testMsg) UnmarshalParquet(obj interfaces.UnmarshalObject) error {
	id, err := obj.GetField("id").Int64()
	if err != nil {
		return err
	}
	m.ID = id
	list, err := obj.GetField("foobar").List()
	if err == interfaces.ErrFieldNotPresent {
		return nil
	}
	if err != nil {
		return err
	}

	for list.Next() {
		v, err := list.Value()
		if err != nil {
			return err
		}
		vv, err := v.ByteArray()
		if err != nil {
			return err
		}
		m.Foobar = append(m.Foobar, string(vv))
	}

	return nil
}

func TestWriteEmptyList(t *testing.T) {
	_ = os.Mkdir("files", 0755)

	sd, err := parquetschema.ParseSchemaDefinition(
		`message test_msg {
			required int64 id;
			optional group foobar (LIST) {
				repeated group list {
					required binary element (STRING);
				}
			}
		}`)
	require.NoError(t, err, "parsing schema definition failed")

	t.Logf("schema definition: %s", spew.Sdump(sd))

	hlWriter, err := NewFileWriter(
		"files/emptylist.parquet",
		goparquet.WithCompressionCodec(parquet.CompressionCodec_SNAPPY),
		goparquet.WithCreator("floor-unittest"),
		goparquet.WithSchemaDefinition(sd),
	)
	require.NoError(t, err, "creating new file writer failed")

	testData1 := &testMsg{ID: 23, Foobar: nil}
	require.NoError(t, hlWriter.Write(testData1), "writing object using marshaller failed")

	testData2 := &testMsg{ID: 42, Foobar: []string{"so", "long", "and", "thanks", "for", "all", "the", "fish"}}
	require.NoError(t, hlWriter.Write(testData2), "writing object using marshaller failed")

	require.NoError(t, hlWriter.Write(testData1), "writing object using marshaller failed")
	require.NoError(t, hlWriter.Write(testData2), "writing object using marshaller failed")

	require.NoError(t, hlWriter.Close())

	hlReader, err := NewFileReader("files/emptylist.parquet")
	require.NoError(t, err, "opening file failed")

	require.True(t, hlReader.Next())

	readData1 := &testMsg{}
	require.NoError(t, hlReader.Scan(readData1))
	require.Equal(t, testData1, readData1, "written and read data don't match")

	readData2 := &testMsg{}
	require.NoError(t, hlReader.Scan(readData2))
	require.Equal(t, testData1, readData2, "written and read data don't match")

	readData3 := &testMsg{}
	require.NoError(t, hlReader.Scan(readData3))
	require.Equal(t, testData1, readData3, "written and read data don't match")

	readData4 := &testMsg{}
	require.NoError(t, hlReader.Scan(readData4))
	require.Equal(t, testData1, readData4, "written and read data don't match")

	require.NoError(t, hlReader.Close())
}
