package go_parquet

import (
	"bytes"
	"math"
	"math/rand"
	"testing"

	"github.com/stretchr/testify/assert"
)

func buildData(bitWidth int, l int) []int32 {
	if bitWidth > 32 || bitWidth <= 0 {
		panic("wrong bitwidth")
	}

	max := int32(math.Pow(2, float64(bitWidth)))
	res := make([]int32, l)
	for i := 0; i < l; i++ {
		if bitWidth < 31 {
			res[i] = rand.Int31n(max)
		} else {
			res[i] = rand.Int31()
		}
	}

	return res
}

func TestHybrid(t *testing.T) {
	for i := 1; i < 32; i++ {
		data := &bytes.Buffer{}
		enc := newHybridEncoder(data, i)
		toW := buildData(i, 8*10240)
		err := enc.encode(toW)
		assert.NoError(t, err)

		buf2 := bytes.NewReader(data.Bytes())
		dec := newHybridDecoder(i)
		assert.NoError(t, dec.init(buf2))
		var toR []int32
		for {
			d, err := dec.next()
			if err != nil {
				break
			}
			toR = append(toR, d)
		}
		assert.Equal(t, toR, toW)
	}
}
