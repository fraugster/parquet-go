package go_parquet

import (
	"encoding/binary"
	"io"
	"math"

	"github.com/pkg/errors"

	"github.com/fraugster/parquet-go/parquet"
)

type floatPlainDecoder struct {
	r io.Reader
}

func (f *floatPlainDecoder) init(r io.Reader) error {
	f.r = r

	return nil
}

func (f *floatPlainDecoder) decodeValues(dst []interface{}) (int, error) {
	var data uint32
	for i := range dst {
		if err := binary.Read(f.r, binary.LittleEndian, &data); err != nil {
			return i, err
		}
		dst[i] = math.Float32frombits(data)
	}

	return len(dst), nil
}

type floatPlainEncoder struct {
	w io.Writer
}

func (d *floatPlainEncoder) Close() error {
	return nil
}

func (d *floatPlainEncoder) init(w io.Writer) error {
	d.w = w

	return nil
}

func (d *floatPlainEncoder) encodeValues(values []interface{}) error {
	data := make([]uint32, len(values))
	for i := range values {
		data[i] = math.Float32bits(values[i].(float32))
	}

	return binary.Write(d.w, binary.LittleEndian, data)
}

type floatStore struct {
	repTyp   parquet.FieldRepetitionType
	min, max float32
}

func (f *floatStore) reset(rep parquet.FieldRepetitionType) {
	f.repTyp = rep
	f.min = math.MaxFloat32
	f.max = -math.MaxFloat32
}

func (f *floatStore) maxValue() []byte {
	ret := make([]byte, 4)
	binary.LittleEndian.PutUint32(ret, math.Float32bits(f.max))
	return ret
}

func (f *floatStore) minValue() []byte {
	ret := make([]byte, 4)
	binary.LittleEndian.PutUint32(ret, math.Float32bits(f.min))
	return ret
}

func (f *floatStore) setMinMax(j float32) {
	if j < f.min {
		f.min = j
	}
	if j > f.max {
		f.max = j
	}
}

func (f *floatStore) getValues(v interface{}) ([]interface{}, error) {
	var vals []interface{}
	switch typed := v.(type) {
	case float32:
		f.setMinMax(typed)
		vals = []interface{}{typed}
	case []float32:
		if f.repTyp != parquet.FieldRepetitionType_REPEATED {
			return nil, errors.Errorf("the value is not repeated but it is an array")
		}
		vals = make([]interface{}, len(typed))
		for j := range typed {
			f.setMinMax(typed[j])
			vals[j] = typed[j]
		}
	default:
		return nil, errors.Errorf("unsupported type for storing in int32 column %T => %+v", v, v)
	}

	return vals, nil
}
