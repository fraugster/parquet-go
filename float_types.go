package go_parquet

import (
	"encoding/binary"
	"io"
	"math"
)

type floatPlainDecoder struct {
	r io.Reader
}

func (f *floatPlainDecoder) init(r io.Reader) error {
	f.r = r

	return nil
}

func (f *floatPlainDecoder) decodeValues(dst []interface{}) error {
	d := make([]uint32, len(dst))
	if err := binary.Read(f.r, binary.LittleEndian, d); err != nil {
		return err
	}
	for i := range d {
		dst[i] = math.Float32frombits(d[i])
	}
	return nil
}
