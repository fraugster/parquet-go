package goparquet

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

	// There is no point for using dictionary if all values are nil
	if len(cs.values.data) == 0 || len(cs.values.values) == 0 {
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

func (cs *ColumnStore) getNext() (v interface{}, err error) {
	v, err = cs.values.getNextValue()
	if err != nil {
		return nil, err
	}
	return v, nil
}

func (cs *ColumnStore) get(maxD, maxR int32) (interface{}, int32, error) {
	if cs.readPos >= len(cs.rLevels) || cs.readPos >= len(cs.dLevels) {
		return nil, 0, errors.New("out of range")
	}
	dl, _, _ := cs.getDRLevelAt(cs.readPos)
	// this is a null value, increase the read pos, for advancing the rLvl and dLvl but
	// do not touch the dict-store
	if dl < maxD {
		cs.readPos++
		return nil, dl, nil
	}
	v, err := cs.getNext()
	if err != nil {
		return nil, 0, err
	}

	// if this is not repeated just return the value, the result is not an array
	if cs.repTyp != parquet.FieldRepetitionType_REPEATED {
		cs.readPos++
		return v, maxD, err
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
			return ret, maxD, nil
		}
		v, err := cs.getNext()
		if err != nil {
			return nil, maxD, err
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
	params := &ColumnParameters{
		LogicalType:   typ.LogicalType,
		ConvertedType: typ.ConvertedType,
		TypeLength:    typ.TypeLength,
		Scale:         typ.Scale,
		Precision:     typ.Precision,
	}

	switch *typ.Type {
	case parquet.Type_BOOLEAN:
		return newPlainStore(&booleanStore{ColumnParameters: params}), nil
	case parquet.Type_BYTE_ARRAY:
		return newPlainStore(&byteArrayStore{ColumnParameters: params}), nil
	case parquet.Type_FIXED_LEN_BYTE_ARRAY:
		if typ.TypeLength == nil {
			return nil, errors.Errorf("type %s with nil type len", typ.Type)
		}

		return newPlainStore(&byteArrayStore{ColumnParameters: params}), nil

	case parquet.Type_FLOAT:
		return newPlainStore(&floatStore{ColumnParameters: params}), nil
	case parquet.Type_DOUBLE:
		return newPlainStore(&doubleStore{ColumnParameters: params}), nil

	case parquet.Type_INT32:
		return newPlainStore(&int32Store{ColumnParameters: params}), nil
	case parquet.Type_INT64:
		return newPlainStore(&int64Store{ColumnParameters: params}), nil
	case parquet.Type_INT96:
		store := &int96Store{}
		store.ColumnParameters = params
		return newPlainStore(store), nil
	default:
		return nil, errors.Errorf("unsupported type: %s", typ.Type)
	}
}

// NewBooleanStore create new boolean store
// TODO: is it make sense to use dictionary on boolean? its RLE encoded 1 bit per one value.
func NewBooleanStore(enc parquet.Encoding, params *ColumnParameters) (*ColumnStore, error) {
	switch enc {
	case parquet.Encoding_PLAIN, parquet.Encoding_RLE:
	default:
		return nil, errors.Errorf("encoding %q is not supported on this type", enc)
	}
	return newStore(&booleanStore{ColumnParameters: params}, enc, false), nil
}

// NewInt32Store create a new int32 store, the allowDict is a hint, no means no dictionary, but yes means if the data
// is good for dictionary, then yes, otherwise no.
func NewInt32Store(enc parquet.Encoding, allowDict bool, params *ColumnParameters) (*ColumnStore, error) {
	switch enc {
	case parquet.Encoding_PLAIN, parquet.Encoding_DELTA_BINARY_PACKED:
	default:
		return nil, errors.Errorf("encoding %q is not supported on this type", enc)
	}
	return newStore(&int32Store{ColumnParameters: params}, enc, allowDict), nil
}

// NewInt64Store creates a new int64 store
func NewInt64Store(enc parquet.Encoding, allowDict bool, params *ColumnParameters) (*ColumnStore, error) {
	switch enc {
	case parquet.Encoding_PLAIN, parquet.Encoding_DELTA_BINARY_PACKED:
	default:
		return nil, errors.Errorf("encoding %q is not supported on this type", enc)
	}
	return newStore(&int64Store{ColumnParameters: params}, enc, allowDict), nil
}

// NewInt96Store creates a new int96 store
func NewInt96Store(enc parquet.Encoding, allowDict bool, params *ColumnParameters) (*ColumnStore, error) {
	switch enc {
	case parquet.Encoding_PLAIN:
	default:
		return nil, errors.Errorf("encoding %q is not supported on this type", enc)
	}
	store := &int96Store{}
	store.ColumnParameters = params
	return newStore(store, enc, allowDict), nil
}

// NewFloatStore creates a float (float32) store
func NewFloatStore(enc parquet.Encoding, allowDict bool, params *ColumnParameters) (*ColumnStore, error) {
	switch enc {
	case parquet.Encoding_PLAIN:
	default:
		return nil, errors.Errorf("encoding %q is not supported on this type", enc)
	}
	return newStore(&floatStore{ColumnParameters: params}, enc, allowDict), nil
}

// NewDoubleStore creates a double (float64) store
func NewDoubleStore(enc parquet.Encoding, allowDict bool, params *ColumnParameters) (*ColumnStore, error) {
	switch enc {
	case parquet.Encoding_PLAIN:
	default:
		return nil, errors.Errorf("encoding %q is not supported on this type", enc)
	}
	return newStore(&doubleStore{ColumnParameters: params}, enc, allowDict), nil
}

// NewByteArrayStore creates a byte array storage
func NewByteArrayStore(enc parquet.Encoding, allowDict bool, params *ColumnParameters) (*ColumnStore, error) {
	switch enc {
	case parquet.Encoding_PLAIN, parquet.Encoding_DELTA_LENGTH_BYTE_ARRAY, parquet.Encoding_DELTA_BYTE_ARRAY:
	default:
		return nil, errors.Errorf("encoding %q is not supported on this type", enc)
	}
	return newStore(&byteArrayStore{ColumnParameters: params}, enc, allowDict), nil
}

// NewFixedByteArrayStore creates a fixed size byte array storage, all element in this storage should be the same size
func NewFixedByteArrayStore(enc parquet.Encoding, allowDict bool, params *ColumnParameters) (*ColumnStore, error) {
	switch enc {
	case parquet.Encoding_PLAIN, parquet.Encoding_DELTA_LENGTH_BYTE_ARRAY, parquet.Encoding_DELTA_BYTE_ARRAY:
	default:
		return nil, errors.Errorf("encoding %q is not supported on this type", enc)
	}
	if params.TypeLength == nil {
		return nil, errors.New("no length provided")
	}

	if *params.TypeLength <= 0 {
		return nil, errors.Errorf("fix length with len %d is not possible", *params.TypeLength)
	}

	return newStore(&byteArrayStore{
		ColumnParameters: params,
	}, enc, allowDict), nil
}
