package goparquet

import (
	"bytes"
	"math"
	"math/rand"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/stretchr/testify/assert"
)

func buildData(bitWidth int, l int) []int32 {
	if bitWidth > 32 || bitWidth < 0 {
		panic("wrong bitwidth")
	}

	max := int32(math.Pow(2, float64(bitWidth)))
	res := make([]int32, l)
	for i := 0; i < l; i++ {
		if bitWidth == 0 {
			res[i] = 0
		} else if bitWidth < 31 {
			res[i] = rand.Int31n(max)
		} else {
			res[i] = rand.Int31()
		}
	}

	return res
}

func TestHybrid(t *testing.T) {
	for i := 0; i < 32; i++ {
		data := &bytes.Buffer{}
		enc := newHybridEncoder(i)
		assert.NoError(t, enc.initSize(data))
		to1 := buildData(i, 8*10240+5)
		assert.NoError(t, enc.encode(to1))

		to2 := buildData(i, 1000)
		assert.NoError(t, enc.encode(to2))

		assert.NoError(t, enc.Close())

		buf2 := bytes.NewReader(data.Bytes())
		dec := newHybridDecoder(i)
		assert.NoError(t, dec.initSize(buf2))
		var toR []int32
		total := len(to1) + len(to2)
		for j := 0; j < total; j++ {
			d, err := dec.next()
			if err != nil {
				break
			}
			toR = append(toR, d)
		}
		assert.Equal(t, toR, append(to1, to2...))
	}
}

func TestOnlyOne(t *testing.T) {
	data := &packedArray{}
	data.reset(1)
	for i := int32(0); i < 1000; i++ {
		data.appendSingle(i)
	}

	buf := &bytes.Buffer{}
	require.NoError(t, encodeLevelsV1(buf, 1, data))
	read := make([]int32, 1000)
	dec := newHybridDecoder(1)
	require.NoError(t, dec.initSize(bytes.NewReader(buf.Bytes())))
	require.NoError(t, decodeInt32(dec, read))
	require.Equal(t, data.toArray(), read)
}
