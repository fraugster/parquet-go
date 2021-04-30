package floor

import (
	"bytes"
	"reflect"
	"testing"
	"time"

	goparquet "github.com/fraugster/parquet-go"
	"github.com/fraugster/parquet-go/parquet"
	"github.com/fraugster/parquet-go/parquetschema"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func writeReadOne(t *testing.T, o interface{}, schema string) interface{} {
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

			t.Run("Time go type", func(t *testing.T) {
				t.Run("nanos", func(t *testing.T) {
					o := struct{ Val time.Time }{Val: time.Now().UTC()}
					s := `message test {required int64 val (TIMESTAMP(NANOS, true));}`
					assert.EqualValues(t, o, writeReadOne(t, o, s))
				})

				t.Run("micros", func(t *testing.T) {
					o := struct{ Val time.Time }{Val: time.Now().UTC()}
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
				type child struct{ Val int }
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
				o := struct{ Val []int }{Val: []int{1}}
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
				o := struct{ Val map[int]int }{Val: map[int]int{1: 1}}
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

	t.Run("when schema is subset of go type", func(t *testing.T) {
		type FieldsOfAllTypes struct {
			Int            int
			Int8           int8
			Int16          int16
			Int32          int32
			Int64          int64
			Uint           uint
			Uint8          uint8
			Uint16         uint16
			Uint32         uint32
			Uint64         uint64
			Float32        float32
			Float64        float64
			Bool           bool
			String         string
			ByteSlice      []byte
			ByteSliceSized [1]byte
			Struct         struct{}
			Slice          []int
			Map            map[int]int
			Time           time.Time

			Val int
		}
		s := `message test {
			required int64 val;
		}`
		o := FieldsOfAllTypes{Val: 1}
		assert.EqualValues(t, o, writeReadOne(t, o, s))
	})
}
