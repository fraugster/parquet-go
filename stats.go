package goparquet

import (
	"bytes"
)

type nilStats struct{}

func (s *nilStats) minValue() []byte {
	return nil
}

func (s *nilStats) maxValue() []byte {
	return nil
}

func (s *nilStats) reset() {
}

type statistics struct {
	min []byte
	max []byte
}

func (s *statistics) minValue() []byte {
	return s.min
}

func (s *statistics) maxValue() []byte {
	return s.max
}

func (s *statistics) reset() {
	s.min, s.max = nil, nil
}

func (s *statistics) setMinMax(j []byte) {
	if s.max == nil || s.min == nil {
		s.min = j
		s.max = j
		return
	}

	if bytes.Compare(j, s.min) < 0 {
		s.min = j
	}
	if bytes.Compare(j, s.max) > 0 {
		s.max = j
	}
}

type numberStats[T numberType, I internalNumberType[T]] struct {
	impl I

	min T
	max T
}

func newNumberStats[T numberType, I internalNumberType[T]]() *numberStats[T, I] {
	s := &numberStats[T, I]{}
	s.reset()
	return s
}

func (s *numberStats[T, I]) reset() {
	s.min = s.impl.MaxValue()
	s.max = s.impl.MinValue()
}

func (s *numberStats[T, I]) minValue() []byte {
	if s.min == s.impl.MaxValue() {
		return nil
	}
	return s.impl.ToBytes(s.min)
}

func (s *numberStats[T, I]) maxValue() []byte {
	if s.max == s.impl.MinValue() {
		return nil
	}
	return s.impl.ToBytes(s.max)
}

func (s *numberStats[T, I]) setMinMax(j T) {
	if j < s.min {
		s.min = j
	}
	if j > s.max {
		s.max = j
	}
}
