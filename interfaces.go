package go_parquet

import (
	"io"

	"github.com/fraugster/parquet-go/parquet"
)

// ChunkReader is the object used to read a chuck and its data on this chunk
type ChunkReader interface {
	Read([]interface{}) (n int, dLevel []uint16, rLevel []uint16, err error)
}

// pageReader is an internal interface used only internally to read the pages
type pageReader interface {
	init(dDecoder, rDecoder func() levelDecoder, values getValueDecoderFn) error
	read(r io.ReadSeeker, ph *parquet.PageHeader, codec parquet.CompressionCodec) error

	readValues([]interface{}) (n int, dLevel []uint16, rLevel []uint16, err error)
}

type valuesDecoder interface {
	init(io.Reader) error
	// the error io.EOF with the less value is acceptable, any other error is not
	decodeValues([]interface{}) (int, error)
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
