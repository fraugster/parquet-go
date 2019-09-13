package goparquet

import (
	"math"
	"math/rand"
	"testing"

	"github.com/stretchr/testify/require"
)

func newRandomPacked(bw int, size int32) (*packedArray, []int32) {
	max := int32(math.Pow(2, float64(bw)))
	data := make([]int32, size)
	packed := &packedArray{}
	packed.reset(bw)
	for i := int32(0); i < size; i++ {
		data[i] = rand.Int31n(max)
		packed.appendSingle(data[i])
	}
	packed.flush()

	return packed, data
}

func TestPackedArray(t *testing.T) {
	const (
		bw   = 13
		size = 10000
	)
	packed, data := newRandomPacked(bw, size)

	ret := make([]int32, 0, size)
	count := 0
	for {
		at, err := packed.at(count)
		if err != nil {
			break
		}
		ret = append(ret, at)
		count++
	}

	require.Equal(t, size, count)
	require.Equal(t, data, ret)
}

func TestPackedArrayAppendArray(t *testing.T) {
	const bw = 7

	fixed, data1 := newRandomPacked(bw, 8*10)
	notFixed, data2 := newRandomPacked(bw, 8*10+1)

	fixed.appendArray(notFixed)
	require.Equal(t, append(data1, data2...), fixed.toArray())

	fixed, data1 = newRandomPacked(bw, 8*10)
	notFixed, data2 = newRandomPacked(bw, 8*10+1)

	notFixed.appendArray(fixed)
	require.Equal(t, append(data2, data1...), notFixed.toArray())
}
