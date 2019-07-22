package go_parquet

import (
	"io"

	"github.com/pkg/errors"
)

type Int96 [12]byte

type int96PlainDecoder struct {
	r io.Reader
}

func (i *int96PlainDecoder) init(r io.Reader) error {
	i.r = r

	return nil
}

func (i *int96PlainDecoder) decodeValues(dst []interface{}) (int, error) {
	idx := 0
	for range dst {
		var data Int96
		// this one is a little tricky do not use ReadFull here
		n, err := i.r.Read(data[:12])
		// make sure we handle the read data first then handle the error
		if n == 12 {
			dst[idx] = data
			idx++
		}

		if err != nil && (n == 0 || n == 12) {
			return idx, err
		}

		if err != nil {
			return idx, errors.Wrap(err, "not enough byte to read the Int96")
		}
	}
	return len(dst), nil
}

type int96PlainEncoder struct {
	w io.Writer
}

func (i *int96PlainEncoder) Close() error {
	return nil
}

func (i *int96PlainEncoder) init(w io.Writer) error {
	i.w = w

	return nil
}

func (i *int96PlainEncoder) encodeValues(values []interface{}) error {
	data := make([]byte, len(values)*12)
	for j := range values {
		i96 := values[j].(Int96)
		copy(data[j*12:], i96[:12])
	}

	return writeFull(i.w, data)
}
