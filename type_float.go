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

func (f *floatPlainDecoder) decodeValues(dst []interface{}) (int, error) {
	var data uint32
	for i := range dst {
		if err := binary.Read(f.r, binary.LittleEndian, &data); err != nil {
			return i, err
		}
		dst[i] = math.Float32frombits(data)
	}

	return len(dst), nil
}

type floatPlainEncoder struct {
	w io.Writer
}

func (d *floatPlainEncoder) Close() error {
	return nil
}

func (d *floatPlainEncoder) init(w io.Writer) error {
	d.w = w

	return nil
}

func (d *floatPlainEncoder) encodeValues(values []interface{}) error {
	data := make([]uint32, len(values))
	for i := range values {
		data[i] = math.Float32bits(values[i].(float32))
	}

	return binary.Write(d.w, binary.LittleEndian, data)
}
