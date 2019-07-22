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

func (d *doublePlainDecoder) decodeValues(dst []interface{}) (int, error) {
	var data uint64
	for i := range dst {
		if err := binary.Read(d.r, binary.LittleEndian, &data); err != nil {
			return i, err
		}
		dst[i] = math.Float64frombits(data)
	}

	return len(dst), nil
}

type doublePlainEncoder struct {
	w io.Writer
}

func (d *doublePlainEncoder) Close() error {
	return nil
}

func (d *doublePlainEncoder) init(w io.Writer) error {
	d.w = w

	return nil
}

func (d *doublePlainEncoder) encodeValues(values []interface{}) error {
	data := make([]uint64, len(values))
	for i := range values {
		data[i] = math.Float64bits(values[i].(float64))
	}

	return binary.Write(d.w, binary.LittleEndian, data)
}
