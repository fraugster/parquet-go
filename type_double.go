package go_parquet

import (
	"encoding/binary"
	"io"
	"math"

	"github.com/pkg/errors"
	"github.com/fraugster/parquet-go/parquet"
)

type doublePlainDecoder struct {
	r io.Reader
}

func (d *doublePlainDecoder) init(r io.Reader) error {
	d.r = r

	return nil
}

func (d *doublePlainDecoder) decodeValues(dst []interface{}) (int, error) {
	var data uint64
	for i := range dst {
		if err := binary.Read(d.r, binary.LittleEndian, &data); err != nil {
			return i, err
		}
		dst[i] = math.Float64frombits(data)
	}

	return len(dst), nil
}

type doublePlainEncoder struct {
	w io.Writer
}

func (d *doublePlainEncoder) Close() error {
	return nil
}

func (d *doublePlainEncoder) init(w io.Writer) error {
	d.w = w

	return nil
}

func (d *doublePlainEncoder) encodeValues(values []interface{}) error {
	data := make([]uint64, len(values))
	for i := range values {
		data[i] = math.Float64bits(values[i].(float64))
	}

	return binary.Write(d.w, binary.LittleEndian, data)
}

type doubleStore struct {
	repTyp   parquet.FieldRepetitionType
	min, max float64
}

func (f *doubleStore) parquetType() parquet.Type {
	return parquet.Type_DOUBLE
}

func (f *doubleStore) typeLen() *int32 {
	return nil
}

func (f *doubleStore) repetitionType() parquet.FieldRepetitionType {
	return f.repTyp
}

func (f *doubleStore) convertedType() *parquet.ConvertedType {
	return nil
}

func (f *doubleStore) scale() *int32 {
	return nil
}

func (f *doubleStore) precision() *int32 {
	return nil
}

func (f *doubleStore) logicalType() *parquet.LogicalType {
	return nil
}

func (f *doubleStore) reset(rep parquet.FieldRepetitionType) {
	f.repTyp = rep
	f.min = math.MaxFloat64
	f.max = -math.MaxFloat64
}

func (f *doubleStore) maxValue() []byte {
	ret := make([]byte, 8)
	binary.LittleEndian.PutUint64(ret, math.Float64bits(f.max))
	return ret
}

func (f *doubleStore) minValue() []byte {
	ret := make([]byte, 8)
	binary.LittleEndian.PutUint64(ret, math.Float64bits(f.min))
	return ret
}

func (f *doubleStore) setMinMax(j float64) {
	if j < f.min {
		f.min = j
	}
	if j > f.max {
		f.max = j
	}
}

func (f *doubleStore) getValues(v interface{}) ([]interface{}, error) {
	var vals []interface{}
	switch typed := v.(type) {
	case float64:
		f.setMinMax(typed)
		vals = []interface{}{typed}
	case []float64:
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
