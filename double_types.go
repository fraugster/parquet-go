package go_parquet

import (
	"encoding/binary"
	"io"
	"math"
)

type doublePlainDecoder struct {
	r io.Reader
}

func (d *doublePlainDecoder) init(r io.Reader) error {
	d.r = r

	return nil
}

func (d *doublePlainDecoder) decodeValues(dst []interface{}) error {
	data := make([]uint64, len(dst))
	if err := binary.Read(d.r, binary.LittleEndian, data); err != nil {
		return err
	}
	for i := range data {
		dst[i] = math.Float64frombits(data[i])
	}
	return nil
}
