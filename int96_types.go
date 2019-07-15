package go_parquet

import (
	"io"
)

type Int96 [12]byte

type int96PlainDecoder struct {
	r io.Reader
}

func (i *int96PlainDecoder) init(r io.Reader) error {
	i.r = r

	return nil
}

func (i *int96PlainDecoder) decodeValues(dst []interface{}) error {
	for j := range dst {
		var data Int96
		_, err := io.ReadFull(i.r, data[:12])
		if err != nil {
			return err
		}
		dst[j] = data
	}
	return nil
}
