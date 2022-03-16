package goparquet

import (
	"bytes"
	"math/rand"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func buildDataDelta(l int) []int32 {
	res := make([]int32, l)
	for i := 0; i < l; i++ {
		res[i] = rand.Int31()
	}

	return res
}

func TestDelta(t *testing.T) {
	for i := 1; i < 32; i++ {
		data := &bytes.Buffer{}
		enc := &deltaBitPackEncoder[int32, internalInt32]{
			blockSize:      128,
			miniBlockCount: 4,
		}
		assert.NoError(t, enc.init(data))
		to1 := buildDataDelta(8*1024 + 5)
		for _, i := range to1 {
			require.NoError(t, enc.addValue(i))
		}
		assert.NoError(t, enc.Close())

		buf2 := bytes.NewReader(data.Bytes())
		dec := &deltaBitPackDecoder[int32, internalInt32]{
			blockSize:      128,
			miniBlockCount: 4,
		}
		assert.NoError(t, dec.init(buf2))
		var toR []int32
		total := len(to1)
		for j := 0; j < total; j++ {
			d, err := dec.next()
			if err != nil {
				break
			}
			toR = append(toR, d)
		}
		assert.Equal(t, toR, to1)
	}
}
