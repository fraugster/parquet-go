package go_parquet

import (
	"encoding/binary"
	"io"
	"math"

	"github.com/pkg/errors"
)

type floatPlainDecoder struct {
}

func (floatPlainDecoder) decodeValues(r io.Reader, dst interface{}) error {
	switch typed := dst.(type) {
	case []float32:
		d := make([]uint32, len(typed))
		if err := binary.Read(r, binary.LittleEndian, d); err != nil {
			return err
		}
		for i := range d {
			typed[i] = math.Float32frombits(d[i])
		}
		return nil
	case []interface{}:
		d := make([]uint32, len(typed))
		if err := binary.Read(r, binary.LittleEndian, d); err != nil {
			return err
		}
		for i := range d {
			typed[i] = math.Float32frombits(d[i])
		}
		return nil
	}
	return errors.Errorf("type %T is not supported for float", dst)
}
