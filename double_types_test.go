package go_parquet

import (
	"bytes"
	"math/rand"
	"testing"

	"github.com/stretchr/testify/assert"
)

func buildFloatArray(count int) []interface{} {
	ret := make([]interface{}, count)
	for i := range ret {
		ret[i] = rand.Float64()
	}

	return ret
}

func TestDoubleType(t *testing.T) {
	arr1 := buildFloatArray(1000)
	arr2 := buildFloatArray(1000)
	w := &bytes.Buffer{}
	enc := &doublePlainEncoder{}
	assert.NoError(t, enc.init(w))
	assert.NoError(t, enc.encodeValues(arr1))
	assert.NoError(t, enc.encodeValues(arr2))

	ret := make([]interface{}, 1000)
	r := bytes.NewReader(w.Bytes())
	dec := &doublePlainDecoder{}
	assert.NoError(t, dec.init(r))
	assert.NoError(t, dec.decodeValues(ret))
	assert.Equal(t, ret, arr1)
	assert.NoError(t, dec.decodeValues(ret))
	assert.Equal(t, ret, arr2)
}
