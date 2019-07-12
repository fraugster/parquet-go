package go_parquet

import (
	"io"

	"github.com/pkg/errors"
)

type Int96 [12]byte

type int96PlainDecoder struct {
}

func (int96PlainDecoder) decodeValues(r io.Reader, dst interface{}) error {
	switch typed := dst.(type) {
	case []Int96:
		for i := range typed {
			_, err := io.ReadFull(r, typed[i][:12])
			if err != nil {
				return err
			}
		}
	case []interface{}:
		for i := range typed {
			var data Int96
			_, err := io.ReadFull(r, data[:12])
			if err != nil {
				return err
			}
			typed[i] = data
		}
		return nil
	}
	return errors.Errorf("type %T is not supported for double", dst)
}
