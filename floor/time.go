package floor

import (
	"errors"
	"fmt"
	"time"
)

// Time represents a time, with no date information.
type Time struct {
	nsec        int64
	utcAdjusted bool
}

const (
	nsPerSecond      = 1000000000
	hoursPerDay      = 24
	secondsPerMinute = 60
	minutesPerHour   = 60
	secondsPerHour   = secondsPerMinute * minutesPerHour
	nsPerMillisecond = 1000000
	nsPerMicrosecond = 1000
)

// NewTime creates a new time object.
func NewTime(hour, min, sec, nanosec int) (Time, error) {
	if hour < 0 || hour >= hoursPerDay {
		return Time{}, errors.New("invalid hour (must be between 0 and 23)")
	}
	if min < 0 || min >= minutesPerHour {
		return Time{}, errors.New("invalid minute (must be between 0 and 59)")
	}
	if sec < 0 || sec >= secondsPerMinute {
		return Time{}, errors.New("invalid second (must be between 0 and 59)")
	}
	if nanosec < 0 || nanosec >= nsPerSecond {
		return Time{}, fmt.Errorf("invalid nanosecond (must be between 0 and %d)", nsPerSecond-1)
	}

	total := int64((hour*secondsPerHour+min*secondsPerMinute+sec)*nsPerSecond + nanosec)

	return Time{
		nsec: total,
	}, nil
}

// Hour returns the hour of the time value.
func (t Time) Hour() int {
	return int(t.nsec / (secondsPerHour * nsPerSecond))
}

// Minute returns the minute of the time value.
func (t Time) Minute() int {
	return int((t.nsec / (secondsPerMinute * nsPerSecond)) % minutesPerHour)
}

// Second returns the second of the time value.
func (t Time) Second() int {
	return int((t.nsec / nsPerSecond) % secondsPerMinute)
}

// Nanosecond returns the sub-second nanosecond value of the time value.
func (t Time) Nanosecond() int {
	return int(t.nsec % nsPerSecond)
}

// Millisecond returns the sub-second millisecond value of the time value
func (t Time) Millisecond() int {
	return t.Nanosecond() / nsPerMillisecond
}

// Microsecond returns the sub-second microsend value of the time value
func (t Time) Microsecond() int {
	return t.Nanosecond() / nsPerMicrosecond
}

// Nanoseconds returns the total number of nanoseconds in the day.
func (t Time) Nanoseconds() int64 {
	return t.nsec
}

// Milliseconds returns the total number of milliseconds in the day.
func (t Time) Milliseconds() int32 {
	return int32(t.nsec / nsPerMillisecond)
}

// Microseconds returns the total number of microseconds in the day.
func (t Time) Microseconds() int64 {
	return t.nsec / nsPerMicrosecond
}

// TimeFromNanoseconds returns a new Time object based on the provided nanoseconds.
func TimeFromNanoseconds(ns int64) Time {
	return Time{nsec: ns}
}

// TimeFromMicroseconds returns a new Time object based on the provided microseconds.
func TimeFromMicroseconds(micros int64) Time {
	return Time{nsec: micros * nsPerMicrosecond}
}

// TimeFromMilliseconds returns a new Time object based on the provided milliseconds.
func TimeFromMilliseconds(ms int32) Time {
	return Time{nsec: int64(ms) * nsPerMillisecond}
}

func (t Time) String() string {
	str := fmt.Sprintf("%02d:%02d:%02d", t.Hour(), t.Minute(), t.Second())

	if nsec := t.Nanosecond(); nsec != 0 {
		str += fmt.Sprintf(".%09d", nsec)
	}

	return str
}

// UTC returns the same Time, but marked as UTC-adjusted.
func (t Time) UTC() Time {
	return Time{
		nsec:        t.nsec,
		utcAdjusted: true,
	}
}

// Today returns a time.Time, combining the current date with the provided time.
func (t Time) Today() time.Time {
	return t.OnThatDay(time.Now())
}

// OnThatDay returns a time.Time, combining the provided date with the time of this object.
func (t Time) OnThatDay(day time.Time) time.Time {
	loc := time.Local
	if t.utcAdjusted {
		loc = time.UTC
	}

	return time.Date(day.Year(), day.Month(), day.Day(), t.Hour(), t.Minute(), t.Second(), t.Nanosecond(), loc)
}

// MustTime panics if err is not nil, otherwise it returns t.
func MustTime(t Time, err error) Time {
	if err != nil {
		panic(err)
	}
	return t
}
