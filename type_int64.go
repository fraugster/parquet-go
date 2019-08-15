package go_parquet

import (
	"encoding/binary"
	"io"
	"math"

	"github.com/pkg/errors"
	"github.com/fraugster/parquet-go/parquet"
)

type int64PlainDecoder struct {
	unSigned bool
	r        io.Reader
}

func (i *int64PlainDecoder) init(r io.Reader) error {
	i.r = r

	return nil
}

func (i *int64PlainDecoder) decodeValues(dst []interface{}) (int, error) {
	var d int64

	for idx := range dst {
		if err := binary.Read(i.r, binary.LittleEndian, &d); err != nil {
			return idx, err
		}
		if i.unSigned {
			dst[idx] = uint64(d)
		} else {
			dst[idx] = d
		}
	}
	return len(dst), nil
}

type int64PlainEncoder struct {
	unSigned bool
	w        io.Writer
}

func (i *int64PlainEncoder) Close() error {
	return nil
}

func (i *int64PlainEncoder) init(w io.Writer) error {
	i.w = w

	return nil
}

func (i *int64PlainEncoder) encodeValues(values []interface{}) error {
	d := make([]int64, len(values))
	if i.unSigned {
		for i := range values {
			d[i] = int64(values[i].(uint64))
		}
	} else {
		for i := range values {
			d[i] = values[i].(int64)
		}
	}
	return binary.Write(i.w, binary.LittleEndian, d)
}

type int64DeltaBPDecoder struct {
	unSigned bool
	deltaBitPackDecoder64
}

func (d *int64DeltaBPDecoder) decodeValues(dst []interface{}) (int, error) {
	for i := range dst {
		u, err := d.next()
		if err != nil {
			return i, err
		}
		if d.unSigned {
			dst[i] = uint64(u)
		} else {
			dst[i] = u
		}
	}

	return len(dst), nil
}

type int64DeltaBPEncoder struct {
	unSigned bool

	deltaBitPackEncoder64
}

func (d *int64DeltaBPEncoder) encodeValues(values []interface{}) error {
	if d.unSigned {
		for i := range values {
			if err := d.addInt64(int64(values[i].(uint64))); err != nil {
				return err
			}
		}
	} else {
		for i := range values {
			if err := d.addInt64(values[i].(int64)); err != nil {
				return err
			}
		}
	}

	return nil
}

type int64Store struct {
	repTyp   parquet.FieldRepetitionType
	min, max int64
}

func (*int64Store) sizeOf(v interface{}) int {
	return 8
}

func (is *int64Store) parquetType() parquet.Type {
	return parquet.Type_INT64
}

func (is *int64Store) typeLen() *int32 {
	return nil
}

func (is *int64Store) repetitionType() parquet.FieldRepetitionType {
	return is.repTyp
}

func (is *int64Store) convertedType() *parquet.ConvertedType {
	return nil
}

func (is *int64Store) scale() *int32 {
	return nil
}

func (is *int64Store) precision() *int32 {
	return nil
}

func (is *int64Store) logicalType() *parquet.LogicalType {
	return nil
}

func (is *int64Store) reset(rep parquet.FieldRepetitionType) {
	is.repTyp = rep
	is.min = math.MaxInt64
	is.max = math.MinInt64
}

func (is *int64Store) maxValue() []byte {
	ret := make([]byte, 8)
	binary.LittleEndian.PutUint64(ret, uint64(is.max))
	return ret
}

func (is *int64Store) minValue() []byte {
	ret := make([]byte, 8)
	binary.LittleEndian.PutUint64(ret, uint64(is.min))
	return ret
}

func (is *int64Store) setMinMax(j int64) {
	if j < is.min {
		is.min = j
	}
	if j > is.max {
		is.max = j
	}
}

func (is *int64Store) getValues(v interface{}) ([]interface{}, error) {
	var vals []interface{}
	switch typed := v.(type) {
	case int64:
		is.setMinMax(typed)
		vals = []interface{}{typed}
	case []int64:
		if is.repTyp != parquet.FieldRepetitionType_REPEATED {
			return nil, errors.Errorf("the value is not repeated but it is an array")
		}
		vals = make([]interface{}, len(typed))
		for j := range typed {
			is.setMinMax(typed[j])
			vals[j] = typed[j]
		}
	default:
		return nil, errors.Errorf("unsupported type for storing in int64 column %T => %+v", v, v)
	}

	return vals, nil
}

func (*int64Store) append(arrayIn interface{}, value interface{}) interface{} {
	if arrayIn == nil {
		arrayIn = make([]int64, 0, 1)
	}
	return append(arrayIn.([]int64), value.(int64))
}
