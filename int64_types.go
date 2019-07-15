package go_parquet

import (
	"encoding/binary"
	"io"
)

type int64PlainDecoder struct {
	r io.Reader
}

func (i *int64PlainDecoder) init(r io.Reader) error {
	i.r = r

	return nil
}

func (i *int64PlainDecoder) decodeValues(dst []interface{}) error {
	d := make([]int64, len(dst))
	if err := binary.Read(i.r, binary.LittleEndian, d); err != nil {
		return err
	}
	for i := range d {
		dst[i] = d[i]
	}
	return nil
}
