package go_parquet

import (
	"encoding/binary"
	"io"

	"github.com/pkg/errors"
)

type byteArrayPlainDecoder struct {
	length int
}

func (b *byteArrayPlainDecoder) next(r io.Reader) ([]byte, error) {
	var l = int32(b.length)
	if l == 0 {
		if err := binary.Read(r, binary.LittleEndian, &l); err != nil {
			return nil, err
		}
	}

	buf := make([]byte, l)
	_, err := io.ReadFull(r, buf)
	if err != nil {
		return nil, err
	}

	return buf, nil
}

func (b *byteArrayPlainDecoder) decodeValues(r io.Reader, dst interface{}) (err error) {
	switch typed := dst.(type) {
	case [][]byte:
		for i := range typed {
			if typed[i], err = b.next(r); err != nil {
				return
			}
		}
		return nil
	case []interface{}:
		for i := range typed {
			if typed[i], err = b.next(r); err != nil {
				return
			}
		}
		return nil
	}
	return errors.Errorf("type %T is not supported for float", dst)
}
