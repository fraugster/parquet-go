package go_parquet

import (
	"io"

	"github.com/fraugster/parquet-go/parquet"

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
		switch t := values[i].(type) {
		case string: // This is only possible when the internal byteArrayDecoder is dictionary
			values[i] = t
		default: // Any other case should be string
			values[i] = string(t.([]byte))
		}
	}

	return n, err
}

type stringEncoder struct {
	bytesArrayEncoder
}

func (s *stringEncoder) init(w io.Writer) error {
	if s.bytesArrayEncoder == nil {
		return errors.New("you should set the bytes array encoder")
	}
	return s.bytesArrayEncoder.init(w)
}

func (s *stringEncoder) encodeValues(values []interface{}) error {
	converted := make([]interface{}, len(values))
	for i := range values {
		converted[i] = []byte(values[i].(string))
	}

	return s.bytesArrayEncoder.encodeValues(converted)
}

type stringStore struct {
	byteArrayStore
}

func (*stringStore) sizeOf(v interface{}) int {
	return len(v.(string))
}

func (s *stringStore) convertedType() *parquet.ConvertedType {
	t := parquet.ConvertedType_UTF8
	return &t
}

func (s *stringStore) logicalType() *parquet.LogicalType {
	l := &parquet.LogicalType{
		STRING: &parquet.StringType{},
	}

	return l
}

func (s *stringStore) getValues(v interface{}) ([]interface{}, error) {
	var vals []interface{}
	switch typed := v.(type) {
	case string:
		if err := s.setMinMax([]byte(typed)); err != nil {
			return nil, err
		}
		vals = []interface{}{typed}
	case []string:
		if s.repTyp != parquet.FieldRepetitionType_REPEATED {
			return nil, errors.Errorf("the value is not repeated but it is an array")
		}
		vals = make([]interface{}, len(typed))
		for j := range typed {
			if err := s.setMinMax([]byte(typed[j])); err != nil {
				return nil, err
			}
			vals[j] = typed[j]
		}
	default:
		return nil, errors.Errorf("unsupported type for storing in string column %T => %+v", v, v)
	}

	return vals, nil
}

func (*stringStore) append(arrayIn interface{}, value interface{}) interface{} {
	if arrayIn == nil {
		arrayIn = make([]string, 0, 1)
	}
	return append(arrayIn.([]string), value.(string))
}
