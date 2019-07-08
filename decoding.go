package go_parquet

import "io"

type valuesDecoder interface {
	decode(io.Reader, interface{}) error
}

type valuesEncoder interface {
	encode(io.Writer, interface{}) error
}

type dictValuesDecoder interface {
	valuesDecoder

	decodeValues(io.Reader, int) error
}

type dictValuesEncoder interface {
	valuesEncoder

	encodeValues(io.Writer, int) error
}
