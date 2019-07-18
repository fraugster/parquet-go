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

type int32PlainEncoder struct {
	w io.Writer
}

func (i *int32PlainEncoder) Close() error {
	return nil
}

func (i *int32PlainEncoder) init(w io.Writer) error {
	i.w = w

	return nil
}

func (i *int32PlainEncoder) encodeValues(values []interface{}) error {
	d := make([]int32, len(values))
	for i := range values {
		d[i] = values[i].(int32)
	}

	return binary.Write(i.w, binary.LittleEndian, d)
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

type int32DeltaBPEncoder struct {
	deltaBitPackEncoder32
}

func (d *int32DeltaBPEncoder) encodeValues(values []interface{}) error {
	for i := range values {
		if err := d.addInt32(values[i].(int32)); err != nil {
			return err
		}
	}

	return nil
}
