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

	keysDecoder *hybridContext
	decoder     *hybridDecoder
}

func (dd *dictDecoder) init(r io.Reader) error {
	buf := make([]byte, 1)
	if _, err := io.ReadFull(r, buf); err != nil {
		return err
	}
	w := int(buf[0])
	if w < 0 || w > 32 {
		return errors.New("dict: invalid bit width")
	}
	if w != 0 {
		dd.keysDecoder = newHybridContext(r)
		dd.decoder = newHybridDecoder(w)
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

func (dd *dictDecoder) decodeKeys(n int) ([]int32, error) {
	if dd.numValues == 0 || dd.keysDecoder == nil {
		return nil, errors.New("dict: no values can be decoded from an empty dictionary")
	}

	ind := make([]int32, n)
	for i := 0; i < n; i++ {
		k, err := dd.decoder.next(dd.keysDecoder)
		if err != nil {
			return nil, err
		}
		if k < 0 || int(k) >= dd.numValues {
			return nil, fmt.Errorf("dict: invalid index %dd, len(values) = %dd", k, dd.numValues)
		}
		ind[i] = k
	}
	return ind, nil
}
