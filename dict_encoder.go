package go_parquet

import (
	"io"

	"github.com/pkg/errors"
)

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

	if err := writeFull(w, buf); err != nil {
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
