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

func (f *numberPlainDecoder[T, I]) decodeValues(dst []any) (int, error) {
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

func (d *numberPlainEncoder[T, I]) encodeValues(values []any) error {
	return d.impl.EncodeBinaryValues(d.w, values)
}

type numberStore[T numberType, I internalNumberType[T]] struct {
	impl I

	repTyp parquet.FieldRepetitionType

	stats     *numberStats[T, I]
	pageStats *numberStats[T, I]

	*ColumnParameters
}

func (f *numberStore[T, I]) params() *ColumnParameters {
	if f.ColumnParameters == nil {
		panic("ColumnParameters is nil")
	}
	return f.ColumnParameters
}

func (f *numberStore[T, I]) sizeOf(v any) int {
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
	f.stats.reset()
	f.pageStats.reset()
}

func (f *numberStore[T, I]) setMinMax(j T) {
	f.stats.setMinMax(j)
	f.pageStats.setMinMax(j)
}

func (f *numberStore[T, I]) getPageStats() minMaxValues {
	return f.pageStats
}

func (f *numberStore[T, I]) getStats() minMaxValues {
	return f.stats
}

func (f *numberStore[T, I]) getValues(v any) ([]any, error) {
	var t T

	var vals []any
	switch typed := v.(type) {
	case T:
		f.setMinMax(typed)
		vals = []any{typed}
	case []T:
		if f.repTyp != parquet.FieldRepetitionType_REPEATED {
			return nil, fmt.Errorf("the value is not repeated but it is an array")
		}
		vals = make([]any, len(typed))
		for j := range typed {
			f.setMinMax(typed[j])
			vals[j] = typed[j]
		}
	default:
		return nil, fmt.Errorf("unsupported type for storing in %T column: %T => %+v", t, v, v)
	}

	return vals, nil
}

func (*numberStore[T, I]) append(arrayIn any, value any) any {
	if arrayIn == nil {
		arrayIn = make([]T, 0, 1)
	}
	return append(arrayIn.([]T), value.(T))
}
