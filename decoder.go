package go_parquet

import "io"

// valueDecoder is used to decode values from the stream into output
type valueDecoder interface {
	Decode(io.Reader, []interface{})
}
