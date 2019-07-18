package go_parquet

import (
	"bytes"
	"math/rand"
	"testing"

	"github.com/stretchr/testify/require"
)

func buildRandArray(count int, fn func() interface{}) []interface{} {
	ret := make([]interface{}, count)
	for i := range ret {
		ret[i] = fn()
	}

	return ret
}

type testFixtures struct {
	name string
	enc  valuesEncoder
	dec  valuesDecoder
	rand func() interface{}
}

var (
	tests = []testFixtures{
		{
			name: "Int32Plain",
			enc:  &int32PlainEncoder{},
			dec:  &int32PlainDecoder{},
			rand: func() interface{} {
				return int32(rand.Int())
			},
		},
		{
			name: "Int32Delta",
			enc:  &int32DeltaBPEncoder{deltaBitPackEncoder32{blockSize: 128, miniBlockCount: 4}},
			dec:  &int32DeltaBPDecoder{},
			rand: func() interface{} {
				return int32(rand.Int())
			},
		},
		{
			name: "Int64Plain",
			enc:  &int64PlainEncoder{},
			dec:  &int64PlainDecoder{},
			rand: func() interface{} {
				return rand.Int63()
			},
		},
		{
			name: "Int64Delta",
			enc:  &int64DeltaBPEncoder{deltaBitPackEncoder64{blockSize: 128, miniBlockCount: 4}},
			dec:  &int64DeltaBPDecoder{},
			rand: func() interface{} {
				return rand.Int63()
			},
		},
		{
			name: "Int96Plain",
			enc:  &int96PlainEncoder{},
			dec:  &int96PlainDecoder{},
			rand: func() interface{} {
				var data Int96
				for i := 0; i < 12; i++ {
					data[i] = byte(rand.Intn(256))
				}

				return data
			},
		},
		{
			name: "DoublePlain",
			enc:  &doublePlainEncoder{},
			dec:  &doublePlainDecoder{},
			rand: func() interface{} {
				return rand.Float64()
			},
		},
		{
			name: "FloatPlain",
			enc:  &floatPlainEncoder{},
			dec:  &floatPlainDecoder{},
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
				var data Int96
				for i := 0; i < 12; i++ {
					data[i] = byte(rand.Intn(10)) // limit the values
				}

				return data
			},
		},
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
			name: "ByteArray",
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
	}
)

func TestTypes(t *testing.T) {
	for _, data := range tests {
		t.Run(data.name, func(t *testing.T) {
			arr1 := buildRandArray(1000, data.rand)
			arr2 := buildRandArray(1000, data.rand)
			w := &bytes.Buffer{}
			require.NoError(t, data.enc.init(w))
			require.NoError(t, data.enc.encodeValues(arr1))
			require.NoError(t, data.enc.encodeValues(arr2))
			require.NoError(t, data.enc.Close())
			var v []interface{}
			if d, ok := data.enc.(dictValuesEncoder); ok {
				v = d.getValues()
			}
			ret := make([]interface{}, 1000)
			r := bytes.NewReader(w.Bytes())
			if d, ok := data.dec.(dictValuesDecoder); ok {
				d.setValues(v)
			}
			require.NoError(t, data.dec.init(r))
			require.NoError(t, data.dec.decodeValues(ret))
			require.Equal(t, ret, arr1)
			require.NoError(t, data.dec.decodeValues(ret))
			require.Equal(t, ret, arr2)
			// No more data
			require.Error(t, data.dec.decodeValues(ret))
		})
	}
}
