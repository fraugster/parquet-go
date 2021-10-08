package floor

import (
	"bytes"
	"reflect"
	"testing"
	"time"

	goparquet "github.com/fraugster/parquet-go"
	"github.com/fraugster/parquet-go/parquet"
	"github.com/fraugster/parquet-go/parquetschema"
	"github.com/fraugster/parquet-go/parquetschema/autoschema"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func writeReadOne(t *testing.T, o interface{}, schema string) interface{} {
	t.Helper()
	defer func() {
		if r := recover(); r != nil {
			t.Error(r)
		}
	}()
	schemaDef, err := parquetschema.ParseSchemaDefinition(schema)
	require.NoError(t, err)

	var buf bytes.Buffer
	w := NewWriter(goparquet.NewFileWriter(&buf,
		goparquet.WithCompressionCodec(parquet.CompressionCodec_SNAPPY),
		goparquet.WithSchemaDefinition(schemaDef),
	))

	err = w.Write(o)
	require.NoError(t, err)

	err = w.Close()
	require.NoError(t, err)

	fr, err := goparquet.NewFileReader(bytes.NewReader(buf.Bytes()))
	require.NoError(t, err)

	pr := NewReader(fr)

	pr.Next()

	o2val := reflect.New(reflect.TypeOf(o))
	err = pr.Scan(o2val.Interface())
	require.NoError(t, err)

	return o2val.Elem().Interface()
}

func TestWriteRead(t *testing.T) {
	t.Run("by parquet type", func(t *testing.T) {
		t.Run("int64", func(t *testing.T) {
			t.Run("int go type", func(t *testing.T) {
				o := struct{ Val int }{Val: 1}
				s := `message test {required int64 val;}`
				assert.EqualValues(t, o, writeReadOne(t, o, s))
			})

			t.Run("int64 go type", func(t *testing.T) {
				o := struct{ Val int64 }{Val: 1}
				s := `message test {required int64 val;}`
				assert.EqualValues(t, o, writeReadOne(t, o, s))
			})

			t.Run("int32 go type", func(t *testing.T) {
				o := struct{ Val int32 }{Val: 1}
				s := `message test {required int64 val;}`
				assert.EqualValues(t, o, writeReadOne(t, o, s))
			})

			t.Run("int16 go type", func(t *testing.T) {
				o := struct{ Val int16 }{Val: 1}
				s := `message test {required int64 val;}`
				assert.EqualValues(t, o, writeReadOne(t, o, s))
			})

			t.Run("int8 go type", func(t *testing.T) {
				o := struct{ Val int8 }{Val: 1}
				s := `message test {required int64 val;}`
				assert.EqualValues(t, o, writeReadOne(t, o, s))
			})

			t.Run("uint go type", func(t *testing.T) {
				o := struct{ Val uint }{Val: 1}
				s := `message test {required int64 val;}`
				assert.EqualValues(t, o, writeReadOne(t, o, s))
			})

			t.Run("uint64 go type", func(t *testing.T) {
				o := struct{ Val uint64 }{Val: 1}
				s := `message test {required int64 val;}`
				assert.EqualValues(t, o, writeReadOne(t, o, s))
			})

			t.Run("uint32 go type", func(t *testing.T) {
				o := struct{ Val uint32 }{Val: 1}
				s := `message test {required int64 val;}`
				assert.EqualValues(t, o, writeReadOne(t, o, s))
			})

			t.Run("uint16 go type", func(t *testing.T) {
				o := struct{ Val uint16 }{Val: 1}
				s := `message test {required int64 val;}`
				assert.EqualValues(t, o, writeReadOne(t, o, s))
			})

			t.Run("uint8 go type", func(t *testing.T) {
				o := struct{ Val uint8 }{Val: 1}
				s := `message test {required int64 val;}`
				assert.EqualValues(t, o, writeReadOne(t, o, s))
			})

			t.Run("Time go type", func(t *testing.T) {
				t.Run("nanos", func(t *testing.T) {
					o := struct{ Val time.Time }{Val: time.Now().UTC()}
					s := `message test {required int64 val (TIMESTAMP(NANOS, true));}`
					assert.EqualValues(t, o, writeReadOne(t, o, s))
				})

				t.Run("micros", func(t *testing.T) {
					o := struct{ Val time.Time }{Val: time.Now().Truncate(time.Microsecond).UTC()}
					s := `message test {required int64 val (TIMESTAMP(MICROS, true));}`
					assert.EqualValues(t, o, writeReadOne(t, o, s))
				})

				t.Run("millis", func(t *testing.T) {
					o := struct{ Val time.Time }{Val: time.Now().Truncate(time.Millisecond).UTC()}
					s := `message test {required int64 val (TIMESTAMP(MILLIS, true));}`
					assert.EqualValues(t, o, writeReadOne(t, o, s))
				})
			})
		})

		t.Run("int32", func(t *testing.T) {
			t.Run("int go type", func(t *testing.T) {
				o := struct{ Val int }{Val: 1}
				s := `message test {required int32 val;}`
				assert.EqualValues(t, o, writeReadOne(t, o, s))
			})

			t.Run("int64 go type", func(t *testing.T) {
				o := struct{ Val int64 }{Val: 1}
				s := `message test {required int32 val;}`
				assert.EqualValues(t, o, writeReadOne(t, o, s))
			})

			t.Run("int32 go type", func(t *testing.T) {
				o := struct{ Val int32 }{Val: 1}
				s := `message test {required int32 val;}`
				assert.EqualValues(t, o, writeReadOne(t, o, s))
			})

			t.Run("int16 go type", func(t *testing.T) {
				o := struct{ Val int16 }{Val: 1}
				s := `message test {required int32 val;}`
				assert.EqualValues(t, o, writeReadOne(t, o, s))
			})

			t.Run("int8 go type", func(t *testing.T) {
				o := struct{ Val int8 }{Val: 1}
				s := `message test {required int32 val;}`
				assert.EqualValues(t, o, writeReadOne(t, o, s))
			})

			t.Run("uint go type", func(t *testing.T) {
				o := struct{ Val uint }{Val: 1}
				s := `message test {required int32 val;}`
				assert.EqualValues(t, o, writeReadOne(t, o, s))
			})

			t.Run("uint64 go type", func(t *testing.T) {
				o := struct{ Val uint64 }{Val: 1}
				s := `message test {required int32 val;}`
				assert.EqualValues(t, o, writeReadOne(t, o, s))
			})

			t.Run("uint32 go type", func(t *testing.T) {
				o := struct{ Val uint32 }{Val: 1}
				s := `message test {required int32 val;}`
				assert.EqualValues(t, o, writeReadOne(t, o, s))
			})

			t.Run("uint16 go type", func(t *testing.T) {
				o := struct{ Val uint16 }{Val: 1}
				s := `message test {required int32 val;}`
				assert.EqualValues(t, o, writeReadOne(t, o, s))
			})

			t.Run("uint8 go type", func(t *testing.T) {
				o := struct{ Val uint8 }{Val: 1}
				s := `message test {required int32 val;}`
				assert.EqualValues(t, o, writeReadOne(t, o, s))
			})
		})

		t.Run("byte_arrays", func(t *testing.T) {
			t.Run("string go type", func(t *testing.T) {
				o := struct{ Val string }{Val: "1"}
				s := `message test {required binary val;}`
				assert.EqualValues(t, o, writeReadOne(t, o, s))
			})

			t.Run("[]byte go type", func(t *testing.T) {
				o := struct{ Val []byte }{Val: []byte("1")}
				s := `message test {required binary val;}`
				assert.EqualValues(t, o, writeReadOne(t, o, s))
			})

			t.Run("[1]byte go type", func(t *testing.T) {
				o := struct{ Val [1]byte }{Val: [1]byte{'1'}}
				s := `message test {required binary val;}`
				assert.EqualValues(t, o, writeReadOne(t, o, s))
			})
		})

		t.Run("float", func(t *testing.T) {
			t.Run("float32 go type", func(t *testing.T) {
				o := struct{ Val float32 }{Val: 1.1}
				s := `message test {required float val;}`
				assert.EqualValues(t, o, writeReadOne(t, o, s))
			})
		})

		t.Run("double", func(t *testing.T) {
			t.Run("float64 go type", func(t *testing.T) {
				o := struct{ Val float64 }{Val: 1.1}
				s := `message test {required double val;}`
				assert.EqualValues(t, o, writeReadOne(t, o, s))
			})
		})

		t.Run("boolean", func(t *testing.T) {
			t.Run("bool go type", func(t *testing.T) {
				o := struct{ Val bool }{Val: true}
				s := `message test {required boolean val;}`
				assert.EqualValues(t, o, writeReadOne(t, o, s))
			})
		})

		t.Run("groups", func(t *testing.T) {
			t.Run("nested struct go type", func(t *testing.T) {
				type child struct{ Val int64 }
				type parent struct{ Child child }
				o := parent{child{1}}
				s := `message parent {
				required group child {
					required int64 val;
				}
			}`
				assert.EqualValues(t, o, writeReadOne(t, o, s))
			})

			t.Run("slice go type", func(t *testing.T) {
				o := struct{ Val []int64 }{Val: []int64{1}}
				s := `message test {
				required group val (LIST) {
					repeated group list {
						required int64 element;
					}
				}
			}`
				assert.EqualValues(t, o, writeReadOne(t, o, s))
			})

			t.Run("map go type", func(t *testing.T) {
				o := struct{ Val map[int64]int64 }{Val: map[int64]int64{1: 1}}
				s := `message test {
				required group val (MAP) {
					repeated group key_value {
						required int64 key;
						required int64 value;
					}
				}
			}`
				assert.EqualValues(t, o, writeReadOne(t, o, s))
			})
		})
	})

	t.Run("fields not defined in schema are ignored, but do not error", func(t *testing.T) {
		t.Run("int", func(t *testing.T) {
			s := `message test {
				required int64 val;
			}`
			o := struct {
				Val   int64
				extra int
			}{Val: 1, extra: 1}
			e := struct {
				Val   int64
				extra int
			}{Val: 1, extra: 0}
			assert.EqualValues(t, e, writeReadOne(t, o, s))
		})
		t.Run("int64", func(t *testing.T) {
			s := `message test {
				required int64 val;
			}`
			o := struct {
				Val   int64
				extra int64
			}{Val: 1, extra: 1}
			e := struct {
				Val   int64
				extra int64
			}{Val: 1, extra: 0}
			assert.EqualValues(t, e, writeReadOne(t, o, s))
		})
		t.Run("int32", func(t *testing.T) {
			s := `message test {
				required int64 val;
			}`
			o := struct {
				Val   int64
				extra int32
			}{Val: 1, extra: 1}
			e := struct {
				Val   int64
				extra int32
			}{Val: 1, extra: 0}
			assert.EqualValues(t, e, writeReadOne(t, o, s))
		})
		t.Run("int16", func(t *testing.T) {
			s := `message test {
				required int64 val;
			}`
			o := struct {
				Val   int64
				extra int16
			}{Val: 1, extra: 1}
			e := struct {
				Val   int64
				extra int16
			}{Val: 1, extra: 0}
			assert.EqualValues(t, e, writeReadOne(t, o, s))
		})
		t.Run("int8", func(t *testing.T) {
			s := `message test {
				required int64 val;
			}`
			o := struct {
				Val   int64
				extra int8
			}{Val: 1, extra: 1}
			e := struct {
				Val   int64
				extra int8
			}{Val: 1, extra: 0}
			assert.EqualValues(t, e, writeReadOne(t, o, s))
		})
		t.Run("uint", func(t *testing.T) {
			s := `message test {
				required int64 val;
			}`
			o := struct {
				Val   int64
				extra uint
			}{Val: 1, extra: 1}
			e := struct {
				Val   int64
				extra uint
			}{Val: 1, extra: 0}
			assert.EqualValues(t, e, writeReadOne(t, o, s))
		})
		t.Run("uint64", func(t *testing.T) {
			s := `message test {
				required int64 val;
			}`
			o := struct {
				Val   int64
				extra uint64
			}{Val: 1, extra: 1}
			e := struct {
				Val   int64
				extra uint64
			}{Val: 1, extra: 0}
			assert.EqualValues(t, e, writeReadOne(t, o, s))
		})
		t.Run("uint32", func(t *testing.T) {
			s := `message test {
				required int64 val;
			}`
			o := struct {
				Val   int64
				extra uint32
			}{Val: 1, extra: 1}
			e := struct {
				Val   int64
				extra uint32
			}{Val: 1, extra: 0}
			assert.EqualValues(t, e, writeReadOne(t, o, s))
		})
		t.Run("uint16", func(t *testing.T) {
			s := `message test {
				required int64 val;
			}`
			o := struct {
				Val   int64
				extra uint16
			}{Val: 1, extra: 1}
			e := struct {
				Val   int64
				extra uint16
			}{Val: 1, extra: 0}
			assert.EqualValues(t, e, writeReadOne(t, o, s))
		})
		t.Run("uint8", func(t *testing.T) {
			s := `message test {
				required int64 val;
			}`
			o := struct {
				Val   int64
				extra uint8
			}{Val: 1, extra: 1}
			e := struct {
				Val   int64
				extra uint8
			}{Val: 1, extra: 0}
			assert.EqualValues(t, e, writeReadOne(t, o, s))
		})
		t.Run("float32", func(t *testing.T) {
			s := `message test {
				required int64 val;
			}`
			o := struct {
				Val   int64
				extra float32
			}{Val: 1, extra: 1}
			e := struct {
				Val   int64
				extra float32
			}{Val: 1, extra: 0}
			assert.EqualValues(t, e, writeReadOne(t, o, s))
		})
		t.Run("float64", func(t *testing.T) {
			s := `message test {
				required int64 val;
			}`
			o := struct {
				Val   int64
				extra float64
			}{Val: 1, extra: 1}
			e := struct {
				Val   int64
				extra float64
			}{Val: 1, extra: 0}
			assert.EqualValues(t, e, writeReadOne(t, o, s))
		})
		t.Run("bool", func(t *testing.T) {
			s := `message test {
				required int64 val;
			}`
			o := struct {
				Val   int64
				extra bool
			}{Val: 1, extra: true}
			e := struct {
				Val   int64
				extra bool
			}{Val: 1, extra: false}
			assert.EqualValues(t, e, writeReadOne(t, o, s))
		})
		t.Run("[1]byte", func(t *testing.T) {
			s := `message test {
				required int64 val;
			}`
			o := struct {
				Val   int64
				extra [1]byte
			}{Val: 1, extra: [1]byte{'1'}}
			e := struct {
				Val   int64
				extra [1]byte
			}{Val: 1, extra: [1]byte{}}
			assert.EqualValues(t, e, writeReadOne(t, o, s))
		})
		t.Run("[]byte", func(t *testing.T) {
			s := `message test {
				required int64 val;
			}`
			o := struct {
				Val   int64
				extra []byte
			}{Val: 1, extra: []byte{'1'}}
			e := struct {
				Val   int64
				extra []byte
			}{Val: 1, extra: nil}
			assert.EqualValues(t, e, writeReadOne(t, o, s))
		})
		t.Run("string", func(t *testing.T) {
			s := `message test {
				required int64 val;
			}`
			o := struct {
				Val   int64
				extra string
			}{Val: 1, extra: "1"}
			e := struct {
				Val   int64
				extra string
			}{Val: 1, extra: ""}
			assert.EqualValues(t, e, writeReadOne(t, o, s))
		})
		t.Run("[]int", func(t *testing.T) {
			s := `message test {
				required int64 val;
			}`
			o := struct {
				Val   int64
				extra []int
			}{Val: 1, extra: []int{1}}
			e := struct {
				Val   int64
				extra []int
			}{Val: 1, extra: nil}
			assert.EqualValues(t, e, writeReadOne(t, o, s))
		})
		t.Run("struct", func(t *testing.T) {
			s := `message test {
				required int64 val;
			}`
			o := struct {
				Val   int64
				extra struct{ i int }
			}{Val: 1, extra: struct{ i int }{1}}
			e := struct {
				Val   int64
				extra struct{ i int }
			}{Val: 1, extra: struct{ i int }{}}
			assert.EqualValues(t, e, writeReadOne(t, o, s))
		})
		t.Run("time.Time", func(t *testing.T) {
			s := `message test {
				required int64 val;
			}`
			o := struct {
				Val   int64
				extra time.Time
			}{Val: 1, extra: time.Now()}
			e := struct {
				Val   int64
				extra time.Time
			}{Val: 1, extra: time.Time{}}
			assert.EqualValues(t, e, writeReadOne(t, o, s))
		})
		t.Run("map[int]int", func(t *testing.T) {
			s := `message test {
				required int64 val;
			}`
			o := struct {
				Val   int64
				extra map[int]int
			}{Val: 1, extra: map[int]int{1: 1}}
			e := struct {
				Val   int64
				extra map[int]int
			}{Val: 1, extra: nil}
			assert.EqualValues(t, e, writeReadOne(t, o, s))
		})
	})
}

func writeReadOneWithAutoSchema(t *testing.T, o interface{}) interface{} {
	t.Helper()
	defer func() {
		if r := recover(); r != nil {
			t.Error(r)
		}
	}()
	schemaDef, err := autoschema.GenerateSchema(o)
	require.NoError(t, err)
	t.Logf("auto-generated schema: %v", schemaDef)

	var buf bytes.Buffer
	w := NewWriter(goparquet.NewFileWriter(&buf,
		goparquet.WithCompressionCodec(parquet.CompressionCodec_SNAPPY),
		goparquet.WithSchemaDefinition(schemaDef),
	))

	err = w.Write(o)
	require.NoError(t, err)

	err = w.Close()
	require.NoError(t, err)

	fr, err := goparquet.NewFileReader(bytes.NewReader(buf.Bytes()))
	require.NoError(t, err)

	pr := NewReader(fr)

	pr.Next()

	o2val := reflect.New(reflect.TypeOf(o))
	err = pr.Scan(o2val.Interface())
	require.NoError(t, err)

	return o2val.Elem().Interface()
}

func TestWriteReadWithAutoSchema(t *testing.T) {
	t.Run("by parquet type", func(t *testing.T) {
		t.Run("int64", func(t *testing.T) {
			t.Run("int go type", func(t *testing.T) {
				o := struct{ Val int }{Val: 1}
				assert.EqualValues(t, o, writeReadOneWithAutoSchema(t, o))
			})

			t.Run("int64 go type", func(t *testing.T) {
				o := struct{ Val int64 }{Val: 1}
				assert.EqualValues(t, o, writeReadOneWithAutoSchema(t, o))
			})

			t.Run("int32 go type", func(t *testing.T) {
				o := struct{ Val int32 }{Val: 1}
				assert.EqualValues(t, o, writeReadOneWithAutoSchema(t, o))
			})

			t.Run("int16 go type", func(t *testing.T) {
				o := struct{ Val int16 }{Val: 1}
				assert.EqualValues(t, o, writeReadOneWithAutoSchema(t, o))
			})

			t.Run("int8 go type", func(t *testing.T) {
				o := struct{ Val int8 }{Val: 1}
				assert.EqualValues(t, o, writeReadOneWithAutoSchema(t, o))
			})

			t.Run("uint go type", func(t *testing.T) {
				o := struct{ Val uint }{Val: 1}
				assert.EqualValues(t, o, writeReadOneWithAutoSchema(t, o))
			})

			t.Run("uint32 go type", func(t *testing.T) {
				o := struct{ Val uint32 }{Val: 1}
				assert.EqualValues(t, o, writeReadOneWithAutoSchema(t, o))
			})

			t.Run("uint16 go type", func(t *testing.T) {
				o := struct{ Val uint16 }{Val: 1}
				assert.EqualValues(t, o, writeReadOneWithAutoSchema(t, o))
			})

			t.Run("uint8 go type", func(t *testing.T) {
				o := struct{ Val uint8 }{Val: 1}
				assert.EqualValues(t, o, writeReadOneWithAutoSchema(t, o))
			})
		})

		t.Run("int32", func(t *testing.T) {
			t.Run("int go type", func(t *testing.T) {
				o := struct{ Val int }{Val: 1}
				assert.EqualValues(t, o, writeReadOneWithAutoSchema(t, o))
			})

			t.Run("int64 go type", func(t *testing.T) {
				o := struct{ Val int64 }{Val: 1}
				assert.EqualValues(t, o, writeReadOneWithAutoSchema(t, o))
			})

			t.Run("int32 go type", func(t *testing.T) {
				o := struct{ Val int32 }{Val: 1}
				assert.EqualValues(t, o, writeReadOneWithAutoSchema(t, o))
			})

			t.Run("int16 go type", func(t *testing.T) {
				o := struct{ Val int16 }{Val: 1}
				assert.EqualValues(t, o, writeReadOneWithAutoSchema(t, o))
			})

			t.Run("int8 go type", func(t *testing.T) {
				o := struct{ Val int8 }{Val: 1}
				assert.EqualValues(t, o, writeReadOneWithAutoSchema(t, o))
			})

			t.Run("uint go type", func(t *testing.T) {
				o := struct{ Val uint }{Val: 1}
				assert.EqualValues(t, o, writeReadOneWithAutoSchema(t, o))
			})

			t.Run("uint32 go type", func(t *testing.T) {
				o := struct{ Val uint32 }{Val: 1}
				assert.EqualValues(t, o, writeReadOneWithAutoSchema(t, o))
			})

			t.Run("uint16 go type", func(t *testing.T) {
				o := struct{ Val uint16 }{Val: 1}
				assert.EqualValues(t, o, writeReadOneWithAutoSchema(t, o))
			})

			t.Run("uint8 go type", func(t *testing.T) {
				o := struct{ Val uint8 }{Val: 1}
				assert.EqualValues(t, o, writeReadOneWithAutoSchema(t, o))
			})
		})

		t.Run("byte_arrays", func(t *testing.T) {
			t.Run("string go type", func(t *testing.T) {
				o := struct{ Val string }{Val: "1"}
				assert.EqualValues(t, o, writeReadOneWithAutoSchema(t, o))
			})

			t.Run("[]byte go type", func(t *testing.T) {
				o := struct{ Val []byte }{Val: []byte("1")}
				assert.EqualValues(t, o, writeReadOneWithAutoSchema(t, o))
			})

			t.Run("[1]byte go type", func(t *testing.T) {
				o := struct{ Val [1]byte }{Val: [1]byte{'1'}}
				assert.EqualValues(t, o, writeReadOneWithAutoSchema(t, o))
			})
		})

		t.Run("float", func(t *testing.T) {
			t.Run("float32 go type", func(t *testing.T) {
				o := struct{ Val float32 }{Val: 1.1}
				assert.EqualValues(t, o, writeReadOneWithAutoSchema(t, o))
			})
		})

		t.Run("double", func(t *testing.T) {
			t.Run("float64 go type", func(t *testing.T) {
				o := struct{ Val float64 }{Val: 1.1}
				assert.EqualValues(t, o, writeReadOneWithAutoSchema(t, o))
			})
		})

		t.Run("boolean", func(t *testing.T) {
			t.Run("bool go type", func(t *testing.T) {
				o := struct{ Val bool }{Val: true}
				assert.EqualValues(t, o, writeReadOneWithAutoSchema(t, o))
			})
		})

		t.Run("groups", func(t *testing.T) {
			t.Run("nested struct go type", func(t *testing.T) {
				type child struct{ Val int64 }
				type parent struct{ Child child }
				o := parent{child{1}}
				assert.EqualValues(t, o, writeReadOneWithAutoSchema(t, o))
			})

			t.Run("slice go type", func(t *testing.T) {
				o := struct{ Val []int64 }{Val: []int64{1}}
				assert.EqualValues(t, o, writeReadOneWithAutoSchema(t, o))
			})

			t.Run("map go type", func(t *testing.T) {
				o := struct{ Val map[int64]int64 }{Val: map[int64]int64{1: 1}}
				assert.EqualValues(t, o, writeReadOneWithAutoSchema(t, o))
			})
		})
	})
}
