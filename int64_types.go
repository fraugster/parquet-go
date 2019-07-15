package go_parquet

import (
	"encoding/binary"
	"io"
)

type int64PlainDecoder struct {
}

func (int64PlainDecoder) decodeValues(r io.Reader, dst []interface{}) error {
	d := make([]int64, len(dst))
	if err := binary.Read(r, binary.LittleEndian, d); err != nil {
		return err
	}
	for i := range d {
		dst[i] = d[i]
	}
	return nil
}
