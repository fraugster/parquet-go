package go_parquet

import (
	"io"
)

type uuidDecoder struct {
	byteArrayPlainDecoder
}

func (ud *uuidDecoder) init(r io.Reader) error {
	// TODO : Not sure if this is the only supported decoder for UUID
	ud.byteArrayPlainDecoder = byteArrayPlainDecoder{
		length: 16,
	}
	return ud.byteArrayPlainDecoder.init(r)
}

type uuidEncoder struct {
	byteArrayPlainEncoder
}

func (ue *uuidEncoder) init(w io.Writer) error {
	ue.byteArrayPlainEncoder = byteArrayPlainEncoder{
		length: 16,
	}
	return ue.byteArrayPlainEncoder.init(w)
}
