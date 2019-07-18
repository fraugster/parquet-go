package go_parquet

import (
	"io"

	"github.com/fraugster/parquet-go/parquet"
)

// Page is the data page
type Page interface {
	init(dDecoder, rDecoder func() levelDecoder, values getValueDecoderFn) error
	read(r io.ReadSeeker, ph *parquet.PageHeader, codec parquet.CompressionCodec) error

	ReadValues([]interface{}) (n int, dLevel []uint16, rLevel []uint16, err error)
}

type valuesDecoder interface {
	init(io.Reader) error
	decodeValues([]interface{}) error
}

type dictValuesDecoder interface {
	valuesDecoder

	setValues([]interface{})
}

type valuesEncoder interface {
	init(io.Writer) error
	encodeValues([]interface{}) error

	io.Closer
}

type dictValuesEncoder interface {
	valuesEncoder

	getValues() []interface{}
}
