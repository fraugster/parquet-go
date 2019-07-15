package go_parquet

import (
	"encoding/binary"
	"io"
	"math"
)

type floatPlainDecoder struct {
}

func (floatPlainDecoder) decodeValues(r io.Reader, dst []interface{}) error {
	d := make([]uint32, len(dst))
	if err := binary.Read(r, binary.LittleEndian, d); err != nil {
		return err
	}
	for i := range d {
		dst[i] = math.Float32frombits(d[i])
	}
	return nil
}
