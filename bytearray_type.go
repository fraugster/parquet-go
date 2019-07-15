package go_parquet

import (
	"encoding/binary"
	"io"
)

type byteArrayPlainDecoder struct {
	// if the length is set, then this is a fix size array decoder, unless it reads the len first
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

func (b *byteArrayPlainDecoder) decodeValues(r io.Reader, dst []interface{}) (err error) {
	for i := range dst {
		if dst[i], err = b.next(r); err != nil {
			return
		}
	}
	return nil
}
