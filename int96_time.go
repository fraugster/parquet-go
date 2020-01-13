package goparquet

import (
	"encoding/binary"
	"time"
)

const (
	jan011970 = 2440588
	secPerDay = 24 * 60 * 60
)

func timeToJD(t time.Time) (uint32, uint64) {
	days := t.Unix() / secPerDay
	nSecs := t.UnixNano() - (days * secPerDay * int64(time.Second))

	// unix time starts from Jan 1, 1970 AC, this day is 2440588 day after the Jan 1, 4713 BC
	return uint32(days) + jan011970, uint64(nSecs)
}

func jdToTime(jd uint32, nsec uint64) time.Time {
	sec := int64(jd-jan011970) * secPerDay
	return time.Unix(sec, int64(nsec))
}

// Int96ToTime is a utility function to convert go time.Time into a Int96 Julian Date (https://en.wikipedia.org/wiki/Julian_day)
// WARNING: this function is limited to the times after Unix epoch (Jan 01. 1970) and can not convert dates before that.
// This time, does not contains a monotonic clock reading and also time.Unix(sec, nsec) creates time in the current machines
// time zone.
func Int96ToTime(parquetDate [12]byte) time.Time {
	nano := binary.LittleEndian.Uint64(parquetDate[:8])
	dt := binary.LittleEndian.Uint32(parquetDate[8:])

	return jdToTime(dt, nano)
}

// TimeToInt96 convert go time.Time into Int96 Julian Date
func TimeToInt96(t time.Time) [12]byte {
	var parquetDate [12]byte
	days, nSecs := timeToJD(t)
	binary.LittleEndian.PutUint64(parquetDate[:8], nSecs)
	binary.LittleEndian.PutUint32(parquetDate[8:], days)

	return parquetDate
}
