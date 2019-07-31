package go_parquet

import (
	"io"

	"github.com/fraugster/parquet-go/parquet"
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

// TODO: should it be fixed len?
type uuidStore struct {
	byteArrayStore
}

func (*uuidStore) sizeOf(v interface{}) int {
	return 16
}

func (u *uuidStore) typeLen() *int32 {
	// TODO : is that correct?
	l := int32(16)
	return &l
}

func (u uuidStore) logicalType() *parquet.LogicalType {
	return &parquet.LogicalType{
		UUID: &parquet.UUIDType{},
	}
}
