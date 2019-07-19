package go_parquet

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestPrefix(t *testing.T) {
	data := []struct {
		P1, p2 string
		C      int
	}{
		{P1: "hello", p2: "goodbye", C: 0},
		{P1: "hello", p2: "hallo", C: 1},
		{P1: "hello", p2: "hello", C: 5},
		{P1: "hellotoyou", p2: "hello", C: 5},
		{P1: "hello", p2: "hellotoyou", C: 5},
	}

	for _, d := range data {
		assert.Equal(t, d.C, prefix([]byte(d.P1), []byte(d.p2)))
	}
}
