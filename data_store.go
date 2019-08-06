package go_parquet

import (
	"math"

	"github.com/pkg/errors"
	"github.com/fraugster/parquet-go/parquet"
)

// ColumnStore is in memory of column store to buffer the column value before writing into a page and read the values
// in the reader mode
type ColumnStore struct {
	repTyp parquet.FieldRepetitionType

	values *dictStore

	dLevels []int32
	rLevels []int32
	rep     []int

	enc       parquet.Encoding
	allowDict bool
	readPos   int
	typedColumnStore
}

// useDictionary is simply a function to decide to use dictionary or not,
// TODO: the logic here is very simple, we need to rethink it
func (cs *ColumnStore) useDictionary() bool {
	if !cs.allowDict {
		return false
	}
	// TODO: Better number?
	if len(cs.values.data) > math.MaxInt16 {
		return false
	}

	dictLen, noDictLen := cs.values.sizes()
	return dictLen < noDictLen
}

func (cs *ColumnStore) encoding() parquet.Encoding {
	return cs.enc
}

func (cs *ColumnStore) repetitionType() parquet.FieldRepetitionType {
	return cs.repTyp
}

// TODO: pass maxR and maxD
// TODO: need to handle reset without losing the schema. maybe remove te `reset` argument and add a new function?
func (cs *ColumnStore) reset(rep parquet.FieldRepetitionType) {
	if cs.typedColumnStore == nil {
		panic("generic should be used with typed column store")
	}
	cs.repTyp = rep
	if cs.values == nil {
		cs.values = &dictStore{}
	}
	cs.values.init()
	cs.dLevels = cs.dLevels[:0]
	cs.rLevels = cs.rLevels[:0]
	cs.rep = cs.rep[:0]
	cs.readPos = 0

	cs.typedColumnStore.reset(rep)
}

// Add One row, if the value is null, call Add() , if the value is repeated, call all value in array
// the second argument s the definition level
// if there is a data the the result should be true, if there is only null (or empty array), the the result should be false
func (cs *ColumnStore) add(v interface{}, dL uint16, maxRL, rL uint16) (bool, error) {
	// if the current column is repeated, we should increase the maxRL here
	if cs.repTyp == parquet.FieldRepetitionType_REPEATED {
		maxRL++
	}
	if rL > maxRL {
		rL = maxRL
	}
	// the dL is a little tricky. there is some case if the REQUIRED field here are nil (since there is something above
	// them is nil) they can not be the first level, but if they are in the next levels, is actually ok, but the
	// level is one less
	if v == nil {
		cs.dLevels = append(cs.dLevels, int32(dL))
		cs.rLevels = append(cs.rLevels, int32(rL))
		// TODO: the next line is the problem. how I can ignore the nil value here? I need the count to be exact, but nil
		// should I save it in the dictionary?
		cs.values.addValue(nil, 0)
		return false, nil
	}
	vals, err := cs.getValues(v)
	if err != nil {
		return false, err
	}
	if len(vals) == 0 {
		// the MaxRl might be increased in the beginning and increased again in the next call but for nil its not important
		return cs.add(nil, dL, maxRL, rL)
	}

	cs.rep = append(cs.rep, len(vals))
	for i, j := range vals {
		cs.values.addValue(j, cs.sizeOf(j))
		tmp := dL
		if cs.repTyp != parquet.FieldRepetitionType_REQUIRED {
			tmp++
		}
		cs.dLevels = append(cs.dLevels, int32(tmp))
		if i == 0 {
			cs.rLevels = append(cs.rLevels, int32(rL))
		} else {
			cs.rLevels = append(cs.rLevels, int32(maxRL))
		}
	}

	return true, nil
}

// getDRLevelAt return the next rLevel in the read position, if there is no value left, it returns true
// if the position is less than zero, then it returns the current position
func (cs *ColumnStore) getDRLevelAt(pos int) (int32, int32, bool) {
	if pos < 0 {
		pos = cs.readPos
	}
	if pos >= len(cs.rLevels) || pos >= len(cs.dLevels) {
		return 0, 0, true
	}

	return cs.dLevels[pos], cs.rLevels[pos], false
}

func (cs *ColumnStore) getNext(pos int, maxD, maxR int32) (v interface{}, err error) {
	v, err = cs.values.getNextValue()
	if err != nil {
		return nil, err
	}
	return v, nil
}

func (cs *ColumnStore) get(maxD, maxR int32) (interface{}, error) {
	if cs.readPos >= len(cs.rLevels) || cs.readPos >= len(cs.dLevels) {
		return nil, errors.New("out of range")
	}
	v, err := cs.getNext(cs.readPos, maxD, maxR)
	if err != nil {
		return nil, err
	}

	// if this is not repeated just return the value, the result is not an array
	if cs.repTyp != parquet.FieldRepetitionType_REPEATED || v == nil {
		cs.readPos++
		return v, err
	}

	// the first rLevel in current object is always less than maxR (only for the repeated values) // TODO : validate that on first value?
	// the next data in this object, should have maxR as the rLevel. the first rLevel less than maxR means the value
	// is from the next object and we should not touch it in this call

	var ret = cs.typedColumnStore.append(nil, v)
	for {
		cs.readPos++
		_, rl, last := cs.getDRLevelAt(cs.readPos)
		if last || rl < maxR {
			// end of this object
			return ret, nil
		}
		v, err := cs.getNext(cs.readPos, maxD, maxR)
		if err != nil {
			return nil, err
		}

		ret = cs.typedColumnStore.append(ret, v)
	}
}

func newStore(typed typedColumnStore, enc parquet.Encoding, allowDict bool) *ColumnStore {
	return &ColumnStore{
		enc:              enc,
		allowDict:        allowDict,
		typedColumnStore: typed,
	}
}

func newPlainStore(typed typedColumnStore) *ColumnStore {
	return newStore(typed, parquet.Encoding_PLAIN, true)
}

// getValuesStore is internally used for the reader
func getValuesStore(typ *parquet.SchemaElement) (*ColumnStore, error) {
	switch *typ.Type {
	case parquet.Type_BOOLEAN:
		return newPlainStore(&booleanStore{}), nil
	case parquet.Type_BYTE_ARRAY:
		if typ.ConvertedType != nil {
			// Should convert to string? enums are not supported in go, so they are simply string
			if *typ.ConvertedType == parquet.ConvertedType_UTF8 || *typ.ConvertedType == parquet.ConvertedType_ENUM {
				return newPlainStore(&stringStore{}), nil
			}
		}
		if typ.LogicalType != nil && (typ.LogicalType.STRING != nil || typ.LogicalType.ENUM != nil) {
			return newPlainStore(&stringStore{}), nil
		}

		return newPlainStore(&byteArrayStore{}), nil
	case parquet.Type_FIXED_LEN_BYTE_ARRAY:

		if typ.LogicalType != nil && typ.LogicalType.UUID != nil {
			return newPlainStore(&uuidStore{}), nil
		}

		if typ.TypeLength == nil {
			return nil, errors.Errorf("type %s with nil type len", typ.Type)
		}

		return newPlainStore(&byteArrayStore{length: int(*typ.TypeLength)}), nil

	case parquet.Type_FLOAT:
		return newPlainStore(&floatStore{}), nil
	case parquet.Type_DOUBLE:
		return newPlainStore(&doubleStore{}), nil

	case parquet.Type_INT32:
		return newPlainStore(&int32Store{}), nil
	case parquet.Type_INT64:
		return newPlainStore(&int64Store{}), nil
	case parquet.Type_INT96:
		return newPlainStore(&int96Store{}), nil
	default:
		return nil, errors.Errorf("unsupported type: %s", typ.Type)
	}
}

// NewBooleanStore create new boolean store
// TODO: is it make sense to use dictionary on boolean? its RLE encoded 1 bit per one value.
func NewBooleanStore(enc parquet.Encoding) (*ColumnStore, error) {
	switch enc {
	case parquet.Encoding_PLAIN, parquet.Encoding_RLE:
	default:
		return nil, errors.Errorf("encoding %q is not supported on this type", enc)
	}
	return newStore(&booleanStore{}, enc, false), nil
}

// NewInt32Store create a new int32 store, the allowDict is a hint, no means no dictionary, but yes means if the data
// is good for dictionary, then yes, otherwise no.
func NewInt32Store(enc parquet.Encoding, allowDict bool) (*ColumnStore, error) {
	switch enc {
	case parquet.Encoding_PLAIN, parquet.Encoding_DELTA_BINARY_PACKED:
	default:
		return nil, errors.Errorf("encoding %q is not supported on this type", enc)
	}
	return newStore(&int32Store{}, enc, allowDict), nil
}

// NewInt64Store creates a new int64 store
func NewInt64Store(enc parquet.Encoding, allowDict bool) (*ColumnStore, error) {
	switch enc {
	case parquet.Encoding_PLAIN, parquet.Encoding_DELTA_BINARY_PACKED:
	default:
		return nil, errors.Errorf("encoding %q is not supported on this type", enc)
	}
	return newStore(&int64Store{}, enc, allowDict), nil
}

// NewInt96Store creates a new int96 store
func NewInt96Store(enc parquet.Encoding, allowDict bool) (*ColumnStore, error) {
	switch enc {
	case parquet.Encoding_PLAIN:
	default:
		return nil, errors.Errorf("encoding %q is not supported on this type", enc)
	}
	return newStore(&int96Store{}, enc, allowDict), nil
}

// NewFloatStore creates a float (float32) store
func NewFloatStore(enc parquet.Encoding, allowDict bool) (*ColumnStore, error) {
	switch enc {
	case parquet.Encoding_PLAIN:
	default:
		return nil, errors.Errorf("encoding %q is not supported on this type", enc)
	}
	return newStore(&floatStore{}, enc, allowDict), nil
}

// NewDoubleStore creates a double (float64) store
func NewDoubleStore(enc parquet.Encoding, allowDict bool) (*ColumnStore, error) {
	switch enc {
	case parquet.Encoding_PLAIN:
	default:
		return nil, errors.Errorf("encoding %q is not supported on this type", enc)
	}
	return newStore(&doubleStore{}, enc, allowDict), nil
}

// NewByteArrayStore creates a byte array storage
func NewByteArrayStore(enc parquet.Encoding, allowDict bool) (*ColumnStore, error) {
	switch enc {
	case parquet.Encoding_PLAIN, parquet.Encoding_DELTA_LENGTH_BYTE_ARRAY, parquet.Encoding_DELTA_BYTE_ARRAY:
	default:
		return nil, errors.Errorf("encoding %q is not supported on this type", enc)
	}
	return newStore(&byteArrayStore{}, enc, allowDict), nil
}

// NewFixedByteArrayStore creates a fixed size byte array storage, all element in this storage should be the same size
func NewFixedByteArrayStore(enc parquet.Encoding, allowDict bool, l int) (*ColumnStore, error) {
	switch enc {
	case parquet.Encoding_PLAIN, parquet.Encoding_DELTA_LENGTH_BYTE_ARRAY, parquet.Encoding_DELTA_BYTE_ARRAY:
	default:
		return nil, errors.Errorf("encoding %q is not supported on this type", enc)
	}
	if l <= 0 {
		return nil, errors.Errorf("fix length with len %d is not possible", l)
	}

	return newStore(&byteArrayStore{
		length: l,
	}, enc, allowDict), nil
}

// NewStringStore creates a new string storage
func NewStringStore(enc parquet.Encoding, allowDict bool) (*ColumnStore, error) {
	switch enc {
	case parquet.Encoding_PLAIN, parquet.Encoding_DELTA_LENGTH_BYTE_ARRAY, parquet.Encoding_DELTA_BYTE_ARRAY:
	default:
		return nil, errors.Errorf("encoding %q is not supported on this type", enc)
	}
	return newStore(&stringStore{}, enc, allowDict), nil
}

// NewUUIDStore create a new UUID store, the allowDict the allowDict is a hint for using dictionary
func NewUUIDStore(enc parquet.Encoding, allowDict bool) (*ColumnStore, error) {
	switch enc {
	case parquet.Encoding_PLAIN, parquet.Encoding_DELTA_LENGTH_BYTE_ARRAY, parquet.Encoding_DELTA_BYTE_ARRAY:
	default:
		return nil, errors.Errorf("encoding %q is not supported on this type", enc)
	}
	return newStore(&uuidStore{}, enc, allowDict), nil
}
