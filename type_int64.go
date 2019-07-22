package go_parquet

import (
	"encoding/binary"
	"io"
)

type int64PlainDecoder struct {
	unSigned bool
	r        io.Reader
}

func (i *int64PlainDecoder) init(r io.Reader) error {
	i.r = r

	return nil
}

func (i *int64PlainDecoder) decodeValues(dst []interface{}) (int, error) {
	var d int64

	for idx := range dst {
		if err := binary.Read(i.r, binary.LittleEndian, &d); err != nil {
			return idx, err
		}
		if i.unSigned {
			dst[idx] = uint64(d)
		} else {
			dst[idx] = d
		}
	}
	return len(dst), nil
}

type int64PlainEncoder struct {
	unSigned bool
	w        io.Writer
}

func (i *int64PlainEncoder) Close() error {
	return nil
}

func (i *int64PlainEncoder) init(w io.Writer) error {
	i.w = w

	return nil
}

func (i *int64PlainEncoder) encodeValues(values []interface{}) error {
	d := make([]int64, len(values))
	if i.unSigned {
		for i := range values {
			d[i] = int64(values[i].(uint64))
		}
	} else {
		for i := range values {
			d[i] = values[i].(int64)
		}
	}
	return binary.Write(i.w, binary.LittleEndian, d)
}

type int64DeltaBPDecoder struct {
	unSigned bool
	deltaBitPackDecoder64
}

func (d *int64DeltaBPDecoder) decodeValues(dst []interface{}) (int, error) {
	for i := range dst {
		u, err := d.next()
		if err != nil {
			return i, err
		}
		if d.unSigned {
			dst[i] = uint64(u)
		} else {
			dst[i] = u
		}
	}

	return len(dst), nil
}

type int64DeltaBPEncoder struct {
	unSigned bool

	deltaBitPackEncoder64
}

func (d *int64DeltaBPEncoder) encodeValues(values []interface{}) error {
	if d.unSigned {
		for i := range values {
			if err := d.addInt64(int64(values[i].(uint64))); err != nil {
				return err
			}
		}
	} else {
		for i := range values {
			if err := d.addInt64(values[i].(int64)); err != nil {
				return err
			}
		}
	}

	return nil
}
