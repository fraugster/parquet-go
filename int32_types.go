package go_parquet

import (
	"encoding/binary"
	"io"
)

type int32PlainDecoder struct {
	r io.Reader
}

func (i *int32PlainDecoder) init(r io.Reader) error {
	i.r = r

	return nil
}

func (i *int32PlainDecoder) decodeValues(dst []interface{}) error {
	d := make([]int32, len(dst))
	if err := binary.Read(i.r, binary.LittleEndian, d); err != nil {
		return err
	}
	for i := range d {
		dst[i] = d[i]
	}
	return nil
}

type int32DeltaBPDecoder struct {
	deltaBitPackDecoder
}

func (d *int32DeltaBPDecoder) init(r io.Reader) error {
	d.deltaBitPackDecoder.bitWidth = 32
	return d.deltaBitPackDecoder.init(r)
}

func (d *int32DeltaBPDecoder) decodeValues(dst []interface{}) error {
	for i := range dst {
		u, err := d.nextInterface()
		if err != nil {
			return err
		}
		dst[i] = u
	}

	return nil
}
