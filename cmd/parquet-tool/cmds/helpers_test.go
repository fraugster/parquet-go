package cmds

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestHumanToByte(t *testing.T) {
	data := []struct {
		In  string
		Out int64
		Err bool
	}{
		{
			In:  "1000",
			Out: 1000,
		},
		{
			In:  "100KB",
			Out: 100 * 1024,
		},
		{
			In:  "100MB",
			Out: 100 * 1024 * 1024,
		},
		{
			In:  "100GB",
			Out: 100 * 1024 * 1024 * 1024,
		},
		{
			In:  "100TB",
			Out: 100 * 1024 * 1024 * 1024 * 1024,
		},
		{
			In:  "100PB",
			Out: 100 * 1024 * 1024 * 1024 * 1024 * 1024,
		},
		{
			In:  "100KiB",
			Out: 100 * 1000,
		},
		{
			In:  "100MiB",
			Out: 100 * 1000 * 1000,
		},
		{
			In:  "100GiB",
			Out: 100 * 1000 * 1000 * 1000,
		},
		{
			In:  "100TiB",
			Out: 100 * 1000 * 1000 * 1000 * 1000,
		},
		{
			In:  "100PiB",
			Out: 100 * 1000 * 1000 * 1000 * 1000 * 1000,
		},
		{
			In:  "ABCD",
			Err: true,
		},
		{
			In:  "ABCDPiB",
			Err: true,
		},
	}

	for _, fix := range data {
		v, err := humanToByte(fix.In)
		if fix.Err {
			require.Error(t, err)
			continue
		}

		require.Equal(t, fix.Out, v, fix.In)
	}
}
