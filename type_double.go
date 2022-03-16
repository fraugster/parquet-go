package goparquet

import (
	"fmt"

	"github.com/fraugster/parquet-go/parquet"
)

type doubleStore struct {
	repTyp parquet.FieldRepetitionType

	stats     *doubleStats
	pageStats *doubleStats

	*ColumnParameters
}

func (f *doubleStore) getStats() minMaxValues {
	return f.stats
}

func (f *doubleStore) getPageStats() minMaxValues {
	return f.pageStats
}

func (f *doubleStore) params() *ColumnParameters {
	if f.ColumnParameters == nil {
		panic("ColumnParameters is nil")
	}
	return f.ColumnParameters
}

func (*doubleStore) sizeOf(v interface{}) int {
	return 8
}

func (f *doubleStore) parquetType() parquet.Type {
	return parquet.Type_DOUBLE
}

func (f *doubleStore) repetitionType() parquet.FieldRepetitionType {
	return f.repTyp
}

func (f *doubleStore) reset(rep parquet.FieldRepetitionType) {
	f.repTyp = rep
	f.stats.reset()
	f.pageStats.reset()
}

func (f *doubleStore) setMinMax(j float64) {
	f.stats.setMinMax(j)
	f.pageStats.setMinMax(j)
}

func (f *doubleStore) getValues(v interface{}) ([]interface{}, error) {
	var vals []interface{}
	switch typed := v.(type) {
	case float64:
		f.setMinMax(typed)
		vals = []interface{}{typed}
	case []float64:
		if f.repTyp != parquet.FieldRepetitionType_REPEATED {
			return nil, fmt.Errorf("the value is not repeated but it is an array")
		}
		vals = make([]interface{}, len(typed))
		for j := range typed {
			f.setMinMax(typed[j])
			vals[j] = typed[j]
		}
	default:
		return nil, fmt.Errorf("unsupported type for storing in float64 column: %T => %+v", v, v)
	}

	return vals, nil
}

func (*doubleStore) append(arrayIn interface{}, value interface{}) interface{} {
	if arrayIn == nil {
		arrayIn = make([]float64, 0, 1)
	}
	return append(arrayIn.([]float64), value.(float64))
}
