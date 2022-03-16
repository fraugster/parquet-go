package goparquet

import (
	"fmt"
	"io"

	"github.com/fraugster/parquet-go/parquet"
)

type numberPlainDecoder[T numberType, I internalNumberType[T]] struct {
	impl I
	r    io.Reader
}

func (f *numberPlainDecoder[T, I]) init(r io.Reader) error {
	f.r = r

	return nil
}

func (f *numberPlainDecoder[T, I]) decodeValues(dst []interface{}) (int, error) {
	return f.impl.DecodeBinaryValues(f.r, dst)
}

type numberPlainEncoder[T numberType, I internalNumberType[T]] struct {
	impl I
	w    io.Writer
}

func (d *numberPlainEncoder[T, I]) Close() error {
	return nil
}

func (d *numberPlainEncoder[T, I]) init(w io.Writer) error {
	d.w = w

	return nil
}

func (d *numberPlainEncoder[T, I]) encodeValues(values []interface{}) error {
	return d.impl.EncodeBinaryValues(d.w, values)
}

type numberStore[T numberType, I internalNumberType[T]] struct {
	impl I

	repTyp   parquet.FieldRepetitionType
	min, max T

	*ColumnParameters
}

func (f *numberStore[T, I]) params() *ColumnParameters {
	if f.ColumnParameters == nil {
		panic("ColumnParameters is nil")
	}
	return f.ColumnParameters
}

func (f *numberStore[T, I]) sizeOf(v interface{}) int {
	return f.impl.Sizeof()
}

func (f *numberStore[T, I]) parquetType() parquet.Type {
	return f.impl.ParquetType()
}

func (f *numberStore[T, I]) repetitionType() parquet.FieldRepetitionType {
	return f.repTyp
}

func (f *numberStore[T, I]) reset(rep parquet.FieldRepetitionType) {
	f.repTyp = rep
	f.min = f.impl.MaxValue()
	f.max = f.impl.MinValue()
}

func (f *numberStore[T, I]) maxValue() []byte {
	if f.max == f.impl.MinValue() {
		return nil
	}
	return f.impl.ToBytes(f.max)
}

func (f *numberStore[T, I]) minValue() []byte {
	if f.min == f.impl.MaxValue() {
		return nil
	}
	return f.impl.ToBytes(f.min)
}

func (f *numberStore[T, I]) setMinMax(j T) {
	if j < f.min {
		f.min = j
	}
	if j > f.max {
		f.max = j
	}
}

func (f *numberStore[T, I]) getValues(v interface{}) ([]interface{}, error) {
	var t T

	var vals []interface{}
	switch typed := v.(type) {
	case T:
		f.setMinMax(typed)
		vals = []interface{}{typed}
	case []T:
		if f.repTyp != parquet.FieldRepetitionType_REPEATED {
			return nil, fmt.Errorf("the value is not repeated but it is an array")
		}
		vals = make([]interface{}, len(typed))
		for j := range typed {
			f.setMinMax(typed[j])
			vals[j] = typed[j]
		}
	default:
		return nil, fmt.Errorf("unsupported type for storing in %T column: %T => %+v", t, v, v)
	}

	return vals, nil
}

func (*numberStore[T, I]) append(arrayIn interface{}, value interface{}) interface{} {
	if arrayIn == nil {
		arrayIn = make([]T, 0, 1)
	}
	return append(arrayIn.([]T), value.(T))
}
