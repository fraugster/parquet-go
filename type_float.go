package goparquet

import (
	"fmt"

	"github.com/fraugster/parquet-go/parquet"
)

type floatStore struct {
	repTyp parquet.FieldRepetitionType

	stats     *floatStats
	pageStats *floatStats

	*ColumnParameters
}

func (f *floatStore) getStats() minMaxValues {
	return f.stats
}

func (f *floatStore) getPageStats() minMaxValues {
	return f.pageStats
}

func (f *floatStore) params() *ColumnParameters {
	if f.ColumnParameters == nil {
		panic("ColumnParameters is nil")
	}
	return f.ColumnParameters
}

func (*floatStore) sizeOf(v interface{}) int {
	return 4
}

func (f *floatStore) parquetType() parquet.Type {
	return parquet.Type_FLOAT
}

func (f *floatStore) repetitionType() parquet.FieldRepetitionType {
	return f.repTyp
}

func (f *floatStore) reset(rep parquet.FieldRepetitionType) {
	f.repTyp = rep
	f.stats.reset()
	f.pageStats.reset()
}

func (f *floatStore) setMinMax(j float32) {
	f.stats.setMinMax(j)
	f.pageStats.setMinMax(j)
}

func (f *floatStore) getValues(v interface{}) ([]interface{}, error) {
	var vals []interface{}
	switch typed := v.(type) {
	case float32:
		f.setMinMax(typed)
		vals = []interface{}{typed}
	case []float32:
		if f.repTyp != parquet.FieldRepetitionType_REPEATED {
			return nil, fmt.Errorf("the value is not repeated but it is an array")
		}
		vals = make([]interface{}, len(typed))
		for j := range typed {
			f.setMinMax(typed[j])
			vals[j] = typed[j]
		}
	default:
		return nil, fmt.Errorf("unsupported type for storing in float32 column: %T => %+v", v, v)
	}

	return vals, nil
}

func (*floatStore) append(arrayIn interface{}, value interface{}) interface{} {
	if arrayIn == nil {
		arrayIn = make([]float32, 0, 1)
	}
	return append(arrayIn.([]float32), value.(float32))
}
