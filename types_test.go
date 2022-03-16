package goparquet

import (
	"bytes"
	"io"
	"math/rand"
	"reflect"
	"testing"

	"github.com/fraugster/parquet-go/parquet"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func buildRandArray(count int, fn func() interface{}) []interface{} {
	ret := make([]interface{}, count)
	for i := range ret {
		ret[i] = fn()
	}

	return ret
}

type encodingFixtures struct {
	name string
	enc  valuesEncoder
	dec  valuesDecoder
	rand func() interface{}
}

var (
	encFixtures = []encodingFixtures{
		{
			name: "Int32Plain",
			enc:  &numberPlainEncoder[int32, internalInt32]{},
			dec:  &numberPlainDecoder[int32, internalInt32]{},
			rand: func() interface{} {
				return int32(rand.Int())
			},
		},
		{
			name: "Int32Delta",
			enc:  &deltaBitPackEncoder[int32, internalInt32]{blockSize: 128, miniBlockCount: 4},
			dec:  &deltaBitPackDecoder[int32, internalInt32]{},
			rand: func() interface{} {
				return int32(rand.Int())
			},
		},
		{
			name: "Int64Plain",
			enc:  &numberPlainEncoder[int64, internalInt64]{},
			dec:  &numberPlainDecoder[int64, internalInt64]{},
			rand: func() interface{} {
				return rand.Int63()
			},
		},
		{
			name: "Int64Delta",
			enc:  &deltaBitPackEncoder[int64, internalInt64]{blockSize: 128, miniBlockCount: 4},
			dec:  &deltaBitPackDecoder[int64, internalInt64]{},
			rand: func() interface{} {
				return rand.Int63()
			},
		},
		{
			name: "Int96Plain",
			enc:  &int96PlainEncoder{},
			dec:  &int96PlainDecoder{},
			rand: func() interface{} {
				var data [12]byte
				for i := 0; i < 12; i++ {
					data[i] = byte(rand.Intn(256))
				}

				return data
			},
		},
		{
			name: "DoublePlain",
			enc:  &numberPlainEncoder[float64, internalFloat64]{},
			dec:  &numberPlainDecoder[float64, internalFloat64]{},
			rand: func() interface{} {
				return rand.Float64()
			},
		},
		{
			name: "FloatPlain",
			enc:  &numberPlainEncoder[float32, internalFloat32]{},
			dec:  &numberPlainDecoder[float32, internalFloat32]{},
			rand: func() interface{} {
				return rand.Float32()
			},
		},
		{
			name: "BooleanRLE",
			enc:  &booleanRLEEncoder{},
			dec:  &booleanRLEDecoder{},
			rand: func() interface{} {
				return rand.Int()%2 == 0
			},
		},
		{
			name: "BooleanPlain",
			enc:  &booleanPlainEncoder{},
			dec:  &booleanPlainDecoder{},
			rand: func() interface{} {
				return rand.Int()%2 == 0
			},
		},
		/*
			{
				name: "DictionaryInt32",
				enc:  &dictEncoder{},
				dec:  &dictDecoder{},
				rand: func() interface{} {
					return rand.Int31n(100)
				},
			},
			{
				name: "DictionaryInt96",
				enc:  &dictEncoder{},
				dec:  &dictDecoder{},
				rand: func() interface{} {
					var data [12]byte
					for i := 0; i < 12; i++ {
						data[i] = byte(rand.Intn(10)) // limit the values
					}

					return data
				},
			},
		*/
		{
			name: "ByteArrayFixedLen",
			enc:  &byteArrayPlainEncoder{length: 3},
			dec:  &byteArrayPlainDecoder{length: 3},
			rand: func() interface{} {
				return []byte{
					byte(rand.Intn(256)),
					byte(rand.Intn(256)),
					byte(rand.Intn(256)),
				}
			},
		},
		{
			name: "ByteArrayPlain",
			enc:  &byteArrayPlainEncoder{},
			dec:  &byteArrayPlainDecoder{},
			rand: func() interface{} {
				l := rand.Intn(10) + 1 // no zero
				ret := make([]byte, l)
				for i := range ret {
					ret[i] = byte(rand.Intn(256))
				}
				return ret
			},
		},
		{
			name: "ByteArrayDeltaLen",
			enc:  &byteArrayDeltaLengthEncoder{},
			dec:  &byteArrayDeltaLengthDecoder{},
			rand: func() interface{} {
				l := rand.Intn(10) + 1 // no zero
				ret := make([]byte, l)
				for i := range ret {
					ret[i] = byte(rand.Intn(256))
				}
				return ret
			},
		},
		{
			name: "ByteArrayDelta",
			enc:  &byteArrayDeltaEncoder{},
			dec:  &byteArrayDeltaDecoder{},
			rand: func() interface{} {
				l := rand.Intn(10) + 1 // no zero
				ret := make([]byte, l)
				for i := range ret {
					ret[i] = byte(rand.Intn(256))
				}
				return ret
			},
		},
	}
)

func TestTypes(t *testing.T) {
	bufLen := 1000

	bufRead := bufLen + bufLen/2

	for _, data := range encFixtures {
		t.Run(data.name, func(t *testing.T) {
			arr1 := buildRandArray(bufLen, data.rand)
			arr2 := buildRandArray(bufLen, data.rand)
			w := &bytes.Buffer{}
			require.NoError(t, data.enc.init(w))
			require.NoError(t, data.enc.encodeValues(arr1))
			require.NoError(t, data.enc.encodeValues(arr2))
			require.NoError(t, data.enc.Close())
			var v []interface{}
			if d, ok := data.enc.(dictValuesEncoder); ok {
				v = d.getValues()
			}
			ret := make([]interface{}, bufRead)
			r := bytes.NewReader(w.Bytes())
			if d, ok := data.dec.(dictValuesDecoder); ok {
				d.setValues(v)
			}
			require.NoError(t, data.dec.init(r))
			n, err := data.dec.decodeValues(ret)
			require.NoError(t, err)
			require.Equal(t, bufRead, n)
			require.Equal(t, ret[:bufLen], arr1)
			//require.Equal(t, len(ret[bufRead:]), len(arr2[:bufRead-bufLen]))
			require.Equal(t, ret[bufLen:], arr2[:bufRead-bufLen])
			n, err = data.dec.decodeValues(ret)
			require.Equal(t, io.EOF, err)
			require.Equal(t, ret[:n], arr2[bufRead-bufLen:])
		})
	}
}

func convertToInterface(arr interface{}) []interface{} {
	v := reflect.ValueOf(arr)
	ret := make([]interface{}, v.Len())

	for i := 0; i < v.Len(); i++ {
		ret[i] = v.Index(i).Interface()
	}

	return ret
}

func getOne(arr interface{}) interface{} {
	v := reflect.ValueOf(arr)
	if v.Len() < 1 {
		panic("no item in the array")
	}

	return v.Index(0).Interface()
}

type storeFixtures struct {
	name  string
	store *ColumnStore
	rand  func(int) interface{}
}

var (
	stFixtures = []storeFixtures{
		{
			name:  "Int32Store",
			store: mustColumnStore(NewInt32Store(parquet.Encoding_PLAIN, false, &ColumnParameters{})),
			rand: func(n int) interface{} {
				ret := make([]int32, n)
				for i := range ret {
					ret[i] = rand.Int31()
				}
				return ret
			},
		},
		{
			name:  "Int64Store",
			store: mustColumnStore(NewInt64Store(parquet.Encoding_PLAIN, false, &ColumnParameters{})),
			rand: func(n int) interface{} {
				ret := make([]int64, n)
				for i := range ret {
					ret[i] = rand.Int63()
				}
				return ret
			},
		},
		{
			name:  "Float32Store",
			store: mustColumnStore(NewFloatStore(parquet.Encoding_PLAIN, false, &ColumnParameters{})),
			rand: func(n int) interface{} {
				ret := make([]float32, n)
				for i := range ret {
					ret[i] = rand.Float32()
				}
				return ret
			},
		},
		{
			name:  "Float64Store",
			store: mustColumnStore(NewDoubleStore(parquet.Encoding_PLAIN, false, &ColumnParameters{})),
			rand: func(n int) interface{} {
				ret := make([]float64, n)
				for i := range ret {
					ret[i] = rand.Float64()
				}
				return ret
			},
		},
		{
			name:  "Int96Store",
			store: mustColumnStore(NewInt96Store(parquet.Encoding_PLAIN, false, &ColumnParameters{})),
			rand: func(n int) interface{} {
				var data = make([][12]byte, n)
				for c := 0; c < n; c++ {
					for i := 0; i < 12; i++ {
						data[c][i] = byte(rand.Intn(255))
					}
				}
				return data
			},
		},
		{
			name:  "BooleanStore",
			store: mustColumnStore(NewBooleanStore(parquet.Encoding_PLAIN, &ColumnParameters{})),
			rand: func(n int) interface{} {
				ret := make([]bool, n)
				for i := range ret {
					ret[i] = rand.Int()%2 == 0
				}
				return ret
			},
		},
	}
)

func mustColumnStore(store *ColumnStore, err error) *ColumnStore {
	if err != nil {
		panic(err)
	}

	return store
}

func TestStores(t *testing.T) {
	for _, fix := range stFixtures {
		t.Run(fix.name, func(t *testing.T) {
			st := fix.store
			randArr := fix.rand

			st.reset(parquet.FieldRepetitionType_REPEATED, 10, 10)

			data := randArr(3)
			err := st.add(data, 3, 3, 0)
			require.NoError(t, err)

			assert.Equal(t, convertToInterface(data), st.values.getValues())
			// Field is not Required, so def level should be one more
			assert.Equal(t, []int32{4, 4, 4}, st.dLevels.toArray())
			// Field is repeated so the rep level (except for the first one which is the new record)
			// should be one more
			assert.Equal(t, []int32{0, 4, 4}, st.rLevels.toArray())

			err = st.add(randArr(0), 3, 3, 0)
			require.NoError(t, err)
			// No Reset
			assert.Equal(t, convertToInterface(data), st.values.getValues())
			// The new field is nil
			assert.Equal(t, []int32{4, 4, 4, 3}, st.dLevels.toArray())
			assert.Equal(t, []int32{0, 4, 4, 0}, st.rLevels.toArray())

			// One record
			data = randArr(1)
			st.reset(parquet.FieldRepetitionType_REQUIRED, 10, 10)
			err = st.add(getOne(data), 3, 3, 0)
			require.NoError(t, err)

			assert.Equal(t, convertToInterface(data), st.values.getValues())
			// Field is Required, so def level should be exact
			assert.Equal(t, []int32{3}, st.dLevels.toArray())
			assert.Equal(t, []int32{0}, st.rLevels.toArray())

			data2 := randArr(1)
			err = st.add(getOne(data2), 3, 3, 10)
			require.NoError(t, err)
			// No reset
			dArr := []interface{}{getOne(data), getOne(data2)}
			assert.Equal(t, dArr, st.values.getValues())
			// Field is Required, so def level should be exact
			assert.Equal(t, []int32{3, 3}, st.dLevels.toArray())
			// rLevel is more than max, so its max now
			assert.Equal(t, []int32{0, 3}, st.rLevels.toArray())

			// empty array had same effect as nil in repeated, but not in required
			err = st.add(randArr(0), 3, 3, 10)
			assert.Error(t, err)

			// Just exact type and nil
			err = st.add(struct{}{}, 3, 3, 0)
			assert.Error(t, err)

			err = st.add(nil, 3, 3, 0)
			assert.NoError(t, err)

			assert.Equal(t, dArr, st.values.getValues())

			// Field is Required, so def level should be exact
			assert.Equal(t, []int32{3, 3, 3}, st.dLevels.toArray())
			// rLevel is more than max, so its max now
			assert.Equal(t, []int32{0, 3, 0}, st.rLevels.toArray())
		})
	}
}
