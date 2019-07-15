package go_parquet

import (
	"encoding/binary"
	"io"
	"math"
)

type doublePlainDecoder struct {
}

func (doublePlainDecoder) decodeValues(r io.Reader, dst []interface{}) error {
	d := make([]uint64, len(dst))
	if err := binary.Read(r, binary.LittleEndian, d); err != nil {
		return err
	}
	for i := range d {
		dst[i] = math.Float64frombits(d[i])
	}
	return nil
}
