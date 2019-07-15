package go_parquet

import (
	"encoding/binary"
	"io"
)

type byteArrayPlainDecoder struct {
	r io.Reader
	// if the length is set, then this is a fix size array decoder, unless it reads the len first
	length int
}

func (b *byteArrayPlainDecoder) init(r io.Reader) error {
	b.r = r
	return nil
}

func (b *byteArrayPlainDecoder) next() ([]byte, error) {
	var l = int32(b.length)
	if l == 0 {
		if err := binary.Read(b.r, binary.LittleEndian, &l); err != nil {
			return nil, err
		}
	}

	buf := make([]byte, l)
	_, err := io.ReadFull(b.r, buf)
	if err != nil {
		return nil, err
	}

	return buf, nil
}

func (b *byteArrayPlainDecoder) decodeValues(dst []interface{}) (err error) {
	for i := range dst {
		if dst[i], err = b.next(); err != nil {
			return
		}
	}
	return nil
}
