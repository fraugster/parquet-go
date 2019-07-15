package go_parquet

import (
	"io"
)

type Int96 [12]byte

type int96PlainDecoder struct {
}

func (int96PlainDecoder) decodeValues(r io.Reader, dst []interface{}) error {
	for i := range dst {
		var data Int96
		_, err := io.ReadFull(r, data[:12])
		if err != nil {
			return err
		}
		dst[i] = data
	}
	return nil
}
