package goparquet

import (
	"fmt"

	"github.com/fraugster/parquet-go/parquet"
)

type int32DeltaBPDecoder struct {
	deltaBitPackDecoder32
}

func (d *int32DeltaBPDecoder) decodeValues(dst []interface{}) (int, error) {
	for i := range dst {
		u, err := d.next()
		if err != nil {
			return i, err
		}
		dst[i] = u
	}

	return len(dst), nil
}

type int32DeltaBPEncoder struct {
	deltaBitPackEncoder32
}

func (d *int32DeltaBPEncoder) encodeValues(values []interface{}) error {
	for i := range values {
		if err := d.addInt32(values[i].(int32)); err != nil {
			return err
		}
	}

	return nil
}

type int32Store struct {
	repTyp parquet.FieldRepetitionType

	stats     *int32Stats
	pageStats *int32Stats

	*ColumnParameters
}

func (is *int32Store) getStats() minMaxValues {
	return is.stats
}

func (is *int32Store) getPageStats() minMaxValues {
	return is.pageStats
}

func (is *int32Store) params() *ColumnParameters {
	if is.ColumnParameters == nil {
		panic("ColumnParameters is nil")
	}
	return is.ColumnParameters
}

func (*int32Store) sizeOf(v interface{}) int {
	return 4
}

func (is *int32Store) parquetType() parquet.Type {
	return parquet.Type_INT32
}

func (is *int32Store) repetitionType() parquet.FieldRepetitionType {
	return is.repTyp
}

func (is *int32Store) reset(rep parquet.FieldRepetitionType) {
	is.repTyp = rep
	is.stats.reset()
	is.pageStats.reset()
}

func (is *int32Store) setMinMax(j int32) {
	is.stats.setMinMax(j)
	is.pageStats.setMinMax(j)
}

func (is *int32Store) getValues(v interface{}) ([]interface{}, error) {
	var vals []interface{}
	switch typed := v.(type) {
	case int32:
		is.setMinMax(typed)
		vals = []interface{}{typed}
	case []int32:
		if is.repTyp != parquet.FieldRepetitionType_REPEATED {
			return nil, fmt.Errorf("the value is not repeated but it is an array")
		}
		vals = make([]interface{}, len(typed))
		for j := range typed {
			is.setMinMax(typed[j])
			vals[j] = typed[j]
		}
	default:
		return nil, fmt.Errorf("unsupported type for storing in int32 column: %T => %+v", v, v)
	}

	return vals, nil
}

func (*int32Store) append(arrayIn interface{}, value interface{}) interface{} {
	if arrayIn == nil {
		arrayIn = make([]int32, 0, 1)
	}
	return append(arrayIn.([]int32), value.(int32))
}
