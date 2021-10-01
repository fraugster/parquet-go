package goparquet

import (
	"math/bits"

	"github.com/pkg/errors"

	"github.com/fraugster/parquet-go/parquet"
)

// maxValueSize is the maximum number of bytes we will put in a single page. Parquet's
// recommendation is that data pages should be 8KB large. This is 2.5x higher than that
// to account for compression.
const maxValueSizePerPage = 20000

// ColumnStore is the read/write implementation for a column. It buffers a single
// column's data that is to be written to a parquet file, knows how to encode this
// data and will choose an optimal way according to heuristics. It also ensures the
// correct decoding of column data to be read.
type ColumnStore struct {
	typedColumnStore

	repTyp parquet.FieldRepetitionType

	currValues    *dictStore
	currRLevels   *packedArray
	currDLevels   *packedArray
	currRowCount  int32
	currNullCount int32

	completeValues    []*dictStore
	completeDLevels   []*packedArray
	completeRLevels   []*packedArray
	completeRowCounts []int32
	minValues         [][]byte
	maxValues         [][]byte

	enc     parquet.Encoding
	readPos int

	allowDict bool

	skipped bool
}

// useDictionary returns true if a dictionary page should be used to represent this
// columnChunk. Currently, every page will either use the dictionary encoding or every
// page will nto use the dictionary encoding. Parquet supports the individual data pages
// being encoded differently, so we could optimize more here, but it would complicate
// the existing dictionary writing logic so for now we it is all or nothing.
func (cs *ColumnStore) useDictionary() bool {
	if !cs.allowDict {
		return false
	}

	if len(cs.completeValues) == 0 {
		return false
	}

	var dataLength, numValues int
	var totDictLen, totNoDictLen int64
	for _, pageValues := range cs.completeValues {
		dataLength += len(pageValues.data)
		numValues += len(pageValues.values)

		dictLen, noDictLen := pageValues.sizes()
		totDictLen += dictLen
		totNoDictLen += noDictLen
	}

	// There is no point for using dictionary if all values are nil
	if dataLength == 0 || numValues == 0 {
		return false
	}

	return totDictLen < totNoDictLen
}

func (cs *ColumnStore) encoding() parquet.Encoding {
	return cs.enc
}

func (cs *ColumnStore) repetitionType() parquet.FieldRepetitionType {
	return cs.repTyp
}

func (cs *ColumnStore) reset(rep parquet.FieldRepetitionType, maxR, maxD uint16) {
	if cs.typedColumnStore == nil {
		panic("generic should be used with typed column store")
	}
	cs.repTyp = rep
	if cs.currValues == nil {
		cs.currValues = &dictStore{}
		cs.currRLevels = &packedArray{}
		cs.currDLevels = &packedArray{}
	}
	cs.currValues.init()
	cs.currRLevels.reset(bits.Len16(maxR))
	cs.currDLevels.reset(bits.Len16(maxD))

	cs.completeValues = []*dictStore{}
	cs.completeDLevels = []*packedArray{}
	cs.completeRLevels = []*packedArray{}

	cs.readPos = 0
	cs.skipped = false

	cs.typedColumnStore.reset(rep)
}

func (cs *ColumnStore) appendRDLevel(rl, dl uint16) {
	cs.currRLevels.appendSingle(int32(rl))
	cs.currDLevels.appendSingle(int32(dl))
}

// Add One row, if the value is null, call Add() , if the value is repeated, call all value in array
// the second argument s the definition level
// if there is a data the the result should be true, if there is only null (or empty array), the the result should be false
func (cs *ColumnStore) add(v interface{}, dL uint16, maxRL, rL uint16) error {
	// If v is nil we will have already called this function once before
	// and incremented the rowCount, so we do not increment it again here.
	if v != nil {
		cs.currRowCount += 1
	}

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
		cs.appendRDLevel(rL, dL)
		cs.currValues.addValue(nil, 0)
		return nil
	}
	vals, err := cs.getValues(v)
	if err != nil {
		return err
	}
	if len(vals) == 0 {
		// the MaxRl might be increased in the beginning and increased again in the next call but for nil its not important
		return cs.add(nil, dL, maxRL, rL)
	}

	for i, j := range vals {
		cs.currValues.addValue(j, cs.sizeOf(j))
		tmp := dL
		if cs.repTyp != parquet.FieldRepetitionType_REQUIRED {
			tmp++
		}

		if i == 0 {
			cs.appendRDLevel(rL, tmp)
		} else {
			cs.appendRDLevel(maxRL, tmp)
		}
	}

	dictLen, noDictLen := cs.currValues.sizes()
	if dictLen > maxValueSizePerPage && noDictLen > maxValueSizePerPage {
		cs.completeLastPage()
	}

	return nil
}

// getRDLevelAt return the next rLevel in the read position, if there is no value left, it returns true
// if the position is less than zero, then it returns the current position
// NOTE: make sure always r is before d, in any function
func (cs *ColumnStore) getRDLevelAt(pos int) (int32, int32, bool) {
	if pos < 0 {
		pos = cs.readPos
	}
	if pos >= cs.currRLevels.count || pos >= cs.currDLevels.count {
		return 0, 0, true
	}
	dl, err := cs.currDLevels.at(pos)
	if err != nil {
		return 0, 0, true
	}
	rl, err := cs.currRLevels.at(pos)
	if err != nil {
		return 0, 0, true
	}

	return rl, dl, false
}

func (cs *ColumnStore) getNext() (v interface{}, err error) {
	v, err = cs.currValues.getNextValue()
	if err != nil {
		return nil, err
	}
	return v, nil
}

func (cs *ColumnStore) get(maxD, maxR int32) (interface{}, int32, error) {
	if cs.skipped {
		return nil, 0, nil
	}

	if cs.readPos >= cs.currRLevels.count || cs.readPos >= cs.currDLevels.count {
		return nil, 0, errors.New("out of range")
	}
	_, dl, _ := cs.getRDLevelAt(cs.readPos)
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

	// the first rLevel in current object is always less than maxR (only for the repeated values)
	// the next data in this object, should have maxR as the rLevel. the first rLevel less than maxR means the value
	// is from the next object and we should not touch it in this call

	var ret = cs.typedColumnStore.append(nil, v)
	for {
		cs.readPos++
		rl, _, last := cs.getRDLevelAt(cs.readPos)
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

func (cs *ColumnStore) getTotalNonNullValues() int32 {
	tot := cs.currValues.numValues()
	for _, values := range cs.completeValues {
		tot += values.numValues()
	}

	return tot
}

func (cs *ColumnStore) getTotalNullValues() int32 {
	tot := cs.currValues.nullValueCount()
	for _, values := range cs.completeValues {
		tot += values.nullValueCount()
	}

	return tot
}

func (cs *ColumnStore) getTotalDistinctValues() int32 {
	tot := cs.currValues.numDistinctValues()
	for _, values := range cs.completeValues {
		tot += values.numDistinctValues()
	}

	return tot
}

func (cs *ColumnStore) getTotalSize() int64 {
	tot := cs.currValues.size
	for _, values := range cs.completeValues {
		tot += values.size
	}
	return tot
}

func (cs *ColumnStore) completeLastPage() {
	if len(cs.currValues.values) == 0 {
		return
	}

	cs.completeRLevels = append(cs.completeRLevels, cs.currRLevels)
	cs.completeDLevels = append(cs.completeDLevels, cs.currDLevels)
	cs.completeValues = append(cs.completeValues, cs.currValues)
	cs.completeRowCounts = append(cs.completeRowCounts, cs.currRowCount)
	cs.minValues = append(cs.minValues, cs.minValue())
	cs.maxValues = append(cs.maxValues, cs.maxValue())

	repetitionLevelBitWidth := cs.currRLevels.bw
	definitionLevelBitWidth := cs.currDLevels.bw

	cs.typedColumnStore.reset(cs.typedColumnStore.repetitionType())
	cs.currValues = &dictStore{}
	cs.currRLevels = &packedArray{}
	cs.currDLevels = &packedArray{}
	cs.currValues.init()
	cs.currRLevels.reset(repetitionLevelBitWidth)
	cs.currDLevels.reset(definitionLevelBitWidth)
	cs.currRowCount = 0
}

func (cs *ColumnStore) getColumnIndex() *parquet.ColumnIndex {
	// TODO: actually start keeping track of these values as we go.
	nullPages := make([]bool, len(cs.completeValues))
	nullCounts := make([]int64, len(cs.completeValues))

	return &parquet.ColumnIndex{
		NullPages:     nullPages,
		MinValues:     cs.minValues,
		MaxValues:     cs.maxValues,
		BoundaryOrder: parquet.BoundaryOrder_UNORDERED,
		NullCounts:    nullCounts,
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

// NewBooleanStore creates new column store to store boolean values.
func NewBooleanStore(enc parquet.Encoding, params *ColumnParameters) (*ColumnStore, error) {
	switch enc {
	case parquet.Encoding_PLAIN, parquet.Encoding_RLE:
	default:
		return nil, errors.Errorf("encoding %q is not supported on this type", enc)
	}
	return newStore(&booleanStore{ColumnParameters: params}, enc, false), nil
}

// NewInt32Store create a new column store to store int32 values. If allowDict is true,
// then using a dictionary is considered by the column store depending on its heuristics.
// If allowDict is false, a dictionary will never be used to encode the data.
func NewInt32Store(enc parquet.Encoding, allowDict bool, params *ColumnParameters) (*ColumnStore, error) {
	switch enc {
	case parquet.Encoding_PLAIN, parquet.Encoding_DELTA_BINARY_PACKED:
	default:
		return nil, errors.Errorf("encoding %q is not supported on this type", enc)
	}
	return newStore(&int32Store{ColumnParameters: params}, enc, allowDict), nil
}

// NewInt64Store creates a new column store to store int64 values. If allowDict is true,
// then using a dictionary is considered by the column store depending on its heuristics.
// If allowDict is false, a dictionary will never be used to encode the data.
func NewInt64Store(enc parquet.Encoding, allowDict bool, params *ColumnParameters) (*ColumnStore, error) {
	switch enc {
	case parquet.Encoding_PLAIN, parquet.Encoding_DELTA_BINARY_PACKED:
	default:
		return nil, errors.Errorf("encoding %q is not supported on this type", enc)
	}
	return newStore(&int64Store{ColumnParameters: params}, enc, allowDict), nil
}

// NewInt96Store creates a new column store to store int96 values. If allowDict is true,
// then using a dictionary is considered by the column store depending on its heuristics.
// If allowDict is false, a dictionary will never be used to encode the data.
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

// NewFloatStore creates a new column store to store float (float32) values. If allowDict is true,
// then using a dictionary is considered by the column store depending on its heuristics.
// If allowDict is false, a dictionary will never be used to encode the data.
func NewFloatStore(enc parquet.Encoding, allowDict bool, params *ColumnParameters) (*ColumnStore, error) {
	switch enc {
	case parquet.Encoding_PLAIN:
	default:
		return nil, errors.Errorf("encoding %q is not supported on this type", enc)
	}
	return newStore(&floatStore{ColumnParameters: params}, enc, allowDict), nil
}

// NewDoubleStore creates a new column store to store double (float64) values. If allowDict is true,
// then using a dictionary is considered by the column store depending on its heuristics.
// If allowDict is false, a dictionary will never be used to encode the data.
func NewDoubleStore(enc parquet.Encoding, allowDict bool, params *ColumnParameters) (*ColumnStore, error) {
	switch enc {
	case parquet.Encoding_PLAIN:
	default:
		return nil, errors.Errorf("encoding %q is not supported on this type", enc)
	}
	return newStore(&doubleStore{ColumnParameters: params}, enc, allowDict), nil
}

// NewByteArrayStore creates a new column store to store byte arrays. If allowDict is true,
// then using a dictionary is considered by the column store depending on its heuristics.
// If allowDict is false, a dictionary will never be used to encode the data.
func NewByteArrayStore(enc parquet.Encoding, allowDict bool, params *ColumnParameters) (*ColumnStore, error) {
	switch enc {
	case parquet.Encoding_PLAIN, parquet.Encoding_DELTA_LENGTH_BYTE_ARRAY, parquet.Encoding_DELTA_BYTE_ARRAY:
	default:
		return nil, errors.Errorf("encoding %q is not supported on this type", enc)
	}
	return newStore(&byteArrayStore{ColumnParameters: params}, enc, allowDict), nil
}

// NewFixedByteArrayStore creates a new column store to store fixed size byte arrays. If allowDict is true,
// then using a dictionary is considered by the column store depending on its heuristics.
// If allowDict is false, a dictionary will never be used to encode the data.
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
