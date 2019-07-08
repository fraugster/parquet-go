package go_parquet

// This file is based on the code from https://github.com/kostya-sh/parquet-go
// Copyright (c) 2015 Konstantin Shaposhnikov

import (
	"fmt"
	"io"

	"github.com/pkg/errors"
)

type dictDecoder struct {
	vd valuesDecoder

	numValues int

	values interface{}
	ind    []int32

	keysDecoder *hybridDecoder
}

func (dd *dictDecoder) init(r io.Reader) error {
	buf := make([]byte, 1)
	if err := readExactly(r, buf); err != nil {
		return err
	}
	w := int(buf[0])
	if w < 0 || w > 32 {
		return errors.New("dict: invalid bit width")
	}
	if w != 0 {
		dd.keysDecoder = newHybridDecoder(r, w)
	}
	return nil
}

func (dd *dictDecoder) decodeValues(dictBuf io.Reader, values interface{}, numValues int) error {
	dd.numValues = numValues
	if dd.numValues == 0 {
		return nil
	}

	if dd.numValues > 0 && dd.keysDecoder == nil {
		return errors.New("dict: bit-width = 0 for non-empty dictionary")
	}

	if err := dd.vd.decode(dictBuf, values); err != nil {
		return err
	}

	dd.values = values
	return nil
}

func (dd *dictDecoder) decodeKeys(n int) (keys []int32, err error) {
	if dd.numValues == 0 || dd.keysDecoder == nil {
		return nil, errors.New("dict: no values can be decoded from an empty dictionary")
	}
	if n > len(dd.ind) {
		dd.ind = make([]int32, n) // TODO: uint32
	}
	for i := 0; i < n; i++ {
		k, err := dd.keysDecoder.next()
		if err != nil {
			return nil, err
		}
		if k < 0 || int(k) >= dd.numValues {
			return nil, fmt.Errorf("dict: invalid index %dd, len(values) = %dd", k, dd.numValues)
		}
		dd.ind[i] = k
	}
	return dd.ind[:n], nil
}

type dictEncoder struct {
	vd valuesEncoder

	numValues int

	keysEncoder *hybridEncoder
}

func (de *dictEncoder) init(w io.Writer, bitWidth int) error {
	if bitWidth < 0 || bitWidth > 32 {
		return errors.New("dict: invalid bit width")
	}
	buf := []byte{byte(bitWidth)}

	if err := writeExactly(w, buf); err != nil {
		return err
	}

	if bitWidth != 0 {
		de.keysEncoder = newHybridEncoder(w, bitWidth)
	}

	return nil
}

func (de *dictEncoder) encodeValues(dictBuf io.Writer, values interface{}, numValues int) error {
	de.numValues = numValues
	if de.numValues == 0 {
		return nil
	}

	if de.keysEncoder == nil {
		return errors.New("dict: bit-width = 0 for non-empty dictionary")
	}

	if err := de.vd.encode(dictBuf, values); err != nil {
		return err
	}

	return nil
}

func (de *dictEncoder) encodeKeys(keys []int32) error {
	if de.numValues == 0 || de.keysEncoder == nil {
		return errors.New("dict: no values can be decoded from an empty dictionary")
	}

	return de.keysEncoder.encode(keys)
}
