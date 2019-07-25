package go_parquet

import (
	"bytes"
	"io"

	"github.com/fraugster/parquet-go/parquet"

	"github.com/pkg/errors"
)

type Int96 [12]byte

type int96PlainDecoder struct {
	r io.Reader
}

func (i *int96PlainDecoder) init(r io.Reader) error {
	i.r = r

	return nil
}

func (i *int96PlainDecoder) decodeValues(dst []interface{}) (int, error) {
	idx := 0
	for range dst {
		var data Int96
		// this one is a little tricky do not use ReadFull here
		n, err := i.r.Read(data[:])
		// make sure we handle the read data first then handle the error
		if n == 12 {
			dst[idx] = data
			idx++
		}

		if err != nil && (n == 0 || n == 12) {
			return idx, err
		}

		if err != nil {
			return idx, errors.Wrap(err, "not enough byte to read the Int96")
		}
	}
	return len(dst), nil
}

type int96PlainEncoder struct {
	w io.Writer
}

func (i *int96PlainEncoder) Close() error {
	return nil
}

func (i *int96PlainEncoder) init(w io.Writer) error {
	i.w = w

	return nil
}

func (i *int96PlainEncoder) encodeValues(values []interface{}) error {
	data := make([]byte, len(values)*12)
	for j := range values {
		i96 := values[j].(Int96)
		copy(data[j*12:], i96[:])
	}

	return writeFull(i.w, data)
}

type int96Store struct {
	repTyp   parquet.FieldRepetitionType
	cnt      int
	min, max Int96
}

func (is *int96Store) reset(repetitionType parquet.FieldRepetitionType) {
	is.repTyp = repetitionType
	is.cnt = 0
}

func (is *int96Store) maxValue() []byte {
	// TODO: copy?
	return is.max[:]
}

func (is *int96Store) minValue() []byte {
	return is.min[:]
}

func (is *int96Store) setMinMax(j Int96) {
	if is.cnt == 0 {
		is.cnt = 1
		is.min = j
		is.max = j
		return
	}
	// TODO : verify the compare
	if bytes.Compare(j[:], is.min[:]) < 0 {
		is.min = j
	}
	if bytes.Compare(j[:], is.min[:]) > 0 {
		is.max = j
	}
}

func (is *int96Store) getValues(v interface{}) ([]interface{}, error) {
	var vals []interface{}
	switch typed := v.(type) {
	case Int96:
		is.setMinMax(typed)
		vals = []interface{}{typed}
	case []Int96:
		if is.repTyp != parquet.FieldRepetitionType_REPEATED {
			return nil, errors.Errorf("the value is not repeated but it is an array")
		}
		vals = make([]interface{}, len(typed))
		for j := range typed {
			is.setMinMax(typed[j])
			vals[j] = typed[j]
		}
	default:
		return nil, errors.Errorf("unsupported type for storing in int32 column %T => %+v", v, v)
	}

	return vals, nil
}
