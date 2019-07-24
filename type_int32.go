package go_parquet

import (
	"encoding/binary"
	"io"
	"math"

	"github.com/fraugster/parquet-go/parquet"

	"github.com/pkg/errors"
)

type int32PlainDecoder struct {
	unSigned bool
	r        io.Reader
}

func (i *int32PlainDecoder) init(r io.Reader) error {
	i.r = r

	return nil
}

func (i *int32PlainDecoder) decodeValues(dst []interface{}) (int, error) {
	var d int32
	for idx := range dst {
		if err := binary.Read(i.r, binary.LittleEndian, &d); err != nil {
			return idx, err
		}
		if i.unSigned {
			dst[idx] = uint32(d)
		} else {
			dst[idx] = d
		}
	}

	return len(dst), nil
}

type int32PlainEncoder struct {
	unSigned bool
	w        io.Writer
}

func (i *int32PlainEncoder) Close() error {
	return nil
}

func (i *int32PlainEncoder) init(w io.Writer) error {
	i.w = w

	return nil
}

func (i *int32PlainEncoder) encodeValues(values []interface{}) error {
	d := make([]int32, len(values))
	if i.unSigned {
		for i := range values {
			d[i] = int32(values[i].(uint32))
		}
	} else {
		for i := range values {
			d[i] = values[i].(int32)
		}
	}
	return binary.Write(i.w, binary.LittleEndian, d)
}

type int32DeltaBPDecoder struct {
	unSigned bool
	deltaBitPackDecoder32
}

func (d *int32DeltaBPDecoder) decodeValues(dst []interface{}) (int, error) {
	for i := range dst {
		u, err := d.next()
		if err != nil {
			return i, err
		}
		if d.unSigned {
			dst[i] = uint32(u)
		} else {
			dst[i] = u
		}
	}

	return len(dst), nil
}

type int32DeltaBPEncoder struct {
	unSigned bool
	deltaBitPackEncoder32
}

func (d *int32DeltaBPEncoder) encodeValues(values []interface{}) error {
	if d.unSigned {
		for i := range values {
			if err := d.addInt32(int32(values[i].(uint32))); err != nil {
				return err
			}
		}
	} else {
		for i := range values {
			if err := d.addInt32(values[i].(int32)); err != nil {
				return err
			}
		}
	}

	return nil
}

type int32Store struct {
	repTyp parquet.FieldRepetitionType

	values *dictStore

	dLevels []int32
	rLevels []int32
	rep     []int
	min     int32
	max     int32
}

func (is *int32Store) reset(rep parquet.FieldRepetitionType) {
	is.repTyp = rep
	if is.values == nil {
		is.values = &dictStore{}
	}
	is.values.init()
	is.dLevels = is.dLevels[:0]
	is.rLevels = is.rLevels[:0]
	is.rep = is.rep[:0]
	is.min = math.MaxInt32
	is.max = math.MinInt32
}

func (is *int32Store) maxValue() []byte {
	ret := make([]byte, 4)
	binary.LittleEndian.PutUint32(ret, uint32(is.max))
	return ret
}

func (is *int32Store) minValue() []byte {
	ret := make([]byte, 4)
	binary.LittleEndian.PutUint32(ret, uint32(is.min))
	return ret
}

func (is *int32Store) add(v interface{}, dL int16, maxRL, rL int16) (bool, error) {
	// if the current column is repeated, we should increase the maxRL here
	if is.repTyp == parquet.FieldRepetitionType_REPEATED {
		maxRL++
	}
	if rL > maxRL {
		rL = maxRL
	}
	// the dL is a little tricky. there is some case if the REQUIRED field here are nil (since there is something above
	// them is nil) they can not be the first level, but if they are in the next levels, is actually ok, but the
	// level is one less
	if v == nil {
		is.dLevels = append(is.dLevels, int32(dL))
		is.rLevels = append(is.rLevels, int32(rL))
		is.values.addValue(int32(0))
		return false, nil
	}
	var vals []int32
	switch i := v.(type) {
	case int32:
		vals = []int32{i}
	case int: // TODO : remove me
		vals = []int32{int32(i)}
	case []int32:
		vals = i
		if is.repTyp != parquet.FieldRepetitionType_REPEATED {
			return false, errors.Errorf("the value is not repeated but it is an array")
		}
		if len(vals) == 0 {
			return is.add(nil, dL, rL, maxRL)
		}
	default:
		return false, errors.Errorf("unsupported type for storing in int32 column %T => %+v", v, v)
	}

	is.rep = append(is.rep, len(vals))
	for i, j := range vals {
		if j < is.min {
			is.min = j
		}
		if j > is.max {
			is.max = j
		}
		is.values.addValue(j)
		tmp := dL
		if is.repTyp != parquet.FieldRepetitionType_REQUIRED {
			tmp++
		}
		is.dLevels = append(is.dLevels, int32(tmp))
		if i == 0 {
			is.rLevels = append(is.rLevels, int32(rL))
		} else {
			is.rLevels = append(is.rLevels, int32(maxRL))
		}
	}

	return true, nil
}

func (is *int32Store) dictionary() *dictStore {
	return is.values
}

func (is *int32Store) definitionLevels() []int32 {
	return is.dLevels
}

func (is *int32Store) repetitionLevels() []int32 {
	return is.rLevels
}
