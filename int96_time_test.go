package goparquet

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestConvert(t *testing.T) {
	// The round here is essential, since the time in parquet does not contains a monotonic clock reading
	now := time.Now().Round(0)
	arr := []time.Time{
		now,
		now.Add(time.Hour),
		now.Add(-24 * time.Hour),
		now.Add(-240 * time.Hour),
		now.Add(-2400 * time.Hour),
		now.Add(-24000 * time.Hour),
	}

	for i := range arr {
		conv := TimeToInt96(arr[i])
		t2 := Int96ToTime(conv)

		require.Equal(t, arr[i], t2)
	}

	date := [12]byte{00, 0x60, 0xFD, 0x4B, 0x32, 0x29, 0x00, 0x00, 0x59, 0x68, 0x25, 0x00}
	// the generated time is in the current timezone (depends on the machine)
	ts := Int96ToTime(date)
	expected := time.Date(2000, 1, 1, 12, 34, 56, 0, time.UTC)
	require.Equal(t, expected, ts.UTC())
}

func TestTimeAfterUnixEpoch(t *testing.T) {
	var (
		t0, t1 time.Time
	)

	t1, _ = time.Parse(time.RFC3339, "1970-01-01T00:00:01Z")

	require.False(t, IsAfterUnixEpoch(t0))
	require.True(t, IsAfterUnixEpoch(t1))
}
