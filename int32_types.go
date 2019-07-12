package go_parquet

import (
	"encoding/binary"
	"io"

	"github.com/pkg/errors"
)

type int32PlainDecoder struct {
}

func (int32PlainDecoder) decodeValues(r io.Reader, dst interface{}) error {
	switch typed := dst.(type) {
	case []int32:
		return binary.Read(r, binary.LittleEndian, dst)
	case []interface{}:
		d := make([]int32, len(typed))
		if err := binary.Read(r, binary.LittleEndian, d); err != nil {
			return err
		}
		for i := range d {
			typed[i] = d[i]
		}
		return nil
	}
	return errors.Errorf("type %T is not supported for int32", dst)
}
