package go_parquet

import (
	"encoding/binary"
	"io"
)

type int32PlainDecoder struct {
	unSigned bool
	r        io.Reader
}

func (i *int32PlainDecoder) init(r io.Reader) error {
	i.r = r

	return nil
}

func (i *int32PlainDecoder) decodeValues(dst []interface{}) (int, error) {
	var d int32
	for idx := range dst {
		if err := binary.Read(i.r, binary.LittleEndian, &d); err != nil {
			return idx, err
		}
		if i.unSigned {
			dst[idx] = uint32(d)
		} else {
			dst[idx] = d
		}
	}

	return len(dst), nil
}

type int32PlainEncoder struct {
	unSigned bool
	w        io.Writer
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
	if i.unSigned {
		for i := range values {
			d[i] = int32(values[i].(uint32))
		}
	} else {
		for i := range values {
			d[i] = values[i].(int32)
		}
	}
	return binary.Write(i.w, binary.LittleEndian, d)
}

type int32DeltaBPDecoder struct {
	unSigned bool
	deltaBitPackDecoder32
}

func (d *int32DeltaBPDecoder) decodeValues(dst []interface{}) (int, error) {
	for i := range dst {
		u, err := d.next()
		if err != nil {
			return i, err
		}
		if d.unSigned {
			dst[i] = uint32(u)
		} else {
			dst[i] = u
		}
	}

	return len(dst), nil
}

type int32DeltaBPEncoder struct {
	unSigned bool
	deltaBitPackEncoder32
}

func (d *int32DeltaBPEncoder) encodeValues(values []interface{}) error {
	if d.unSigned {
		for i := range values {
			if err := d.addInt32(int32(values[i].(uint32))); err != nil {
				return err
			}
		}
	} else {
		for i := range values {
			if err := d.addInt32(values[i].(int32)); err != nil {
				return err
			}
		}
	}

	return nil
}
