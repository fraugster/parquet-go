package goparquet

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestDictStore(t *testing.T) {
	d := dictStore{}
	d.init()
	require.Equal(t, d.size, int64(0))

	d.addValue(int32(1), 4)
	d.addValue(int32(2), 4)
	d.addValue(int32(3), 4)
	d.addValue(int32(4), 4)
	d.addValue(int32(1), 4)
	d.addValue(int32(2), 4)
	d.addValue(int32(3), 4)
	d.addValue(int32(4), 4)
	require.Equal(t, d.size, int64(32))
	d.addValue(nil, 4)
	require.Equal(t, d.size, int64(32))

	d.init()
	require.Equal(t, d.size, int64(0))
}
