package go_parquet

import (
	"io"

	"github.com/pkg/errors"
)

type stringDecoder struct {
	bytesArrayDecoder
}

func (s *stringDecoder) init(r io.Reader) error {
	if s.bytesArrayDecoder == nil {
		return errors.New("you should set the bytes array decoder")
	}
	return s.bytesArrayDecoder.init(r)
}

func (s *stringDecoder) decodeValues(values []interface{}) (int, error) {
	n, err := s.bytesArrayDecoder.decodeValues(values)
	if err != nil && err != io.EOF {
		return n, err
	}

	for i := 0; i < n; i++ {
		values[i] = string(values[i].([]byte))
	}

	return n, err
}

type stringEncoder struct {
	byteArrayEncoder
}

func (s *stringEncoder) init(w io.Writer) error {
	if s.byteArrayEncoder == nil {
		return errors.New("you should set the bytes array encoder")
	}
	return s.byteArrayEncoder.init(w)
}

func (s *stringEncoder) encodeValues(values []interface{}) error {
	converted := make([]interface{}, len(values))
	for i := range values {
		converted[i] = []byte(values[i].(string))
	}

	return s.byteArrayEncoder.encodeValues(converted)
}
