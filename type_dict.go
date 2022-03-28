package goparquet

import (
	"errors"
	"fmt"
	"io"
)

type dictDecoder struct {
	uniqueValues []interface{}

	keys decoder
}

// just for tests
func (d *dictDecoder) setValues(v []interface{}) {
	d.uniqueValues = v
}

// the value should be there before the init
func (d *dictDecoder) init(r io.Reader) error {
	buf := make([]byte, 1)
	if _, err := io.ReadFull(r, buf); err != nil {
		return err
	}
	w := int(buf[0])
	if w < 0 || w > 32 {
		return fmt.Errorf("invalid bitwidth %d", w)
	}
	if w >= 0 {
		d.keys = newHybridDecoder(w)
		err := d.keys.init(r)
		return err
	}

	return errors.New("bit width zero with non-empty dictionary")
}

func (d *dictDecoder) decodeValues(dst []interface{}) (int, error) {
	if d.keys == nil {
		return 0, errors.New("no value is inside dictionary")
	}
	size := int32(len(d.uniqueValues))

	for i := range dst {
		key, err := d.keys.next()
		if err != nil {
			return i, err
		}

		if key < 0 || key >= size {
			return 0, fmt.Errorf("dict: invalid index %d, values count are %d", key, size)
		}

		dst[i] = d.uniqueValues[key]
	}

	return len(dst), nil
}

type dictStore struct {
	valueList        []interface{}
	uniqueValues     map[interface{}]struct{}
	uniqueValuesSize int64
	allValuesSize    int64
	readPos          int
	nullCount        int32
}

func (d *dictStore) getValues() []interface{} {
	return d.valueList
}

func (d *dictStore) init() {
	d.uniqueValues = make(map[interface{}]struct{})
	d.valueList = nil
	d.reset()
}

func (d *dictStore) reset() {
	d.nullCount = 0
	d.readPos = 0
	d.uniqueValuesSize = 0
	d.allValuesSize = 0
}

func (d *dictStore) addValue(v interface{}, size int) {
	if v == nil {
		d.nullCount++
		return
	}
	k := mapKey(v)
	if _, found := d.uniqueValues[k]; !found {
		d.uniqueValues[k] = struct{}{}
		d.uniqueValuesSize += int64(size)
	}
	d.allValuesSize += int64(size)
	d.valueList = append(d.valueList, v)
}

func (d *dictStore) getNextValue() (interface{}, error) {
	if d.readPos >= len(d.valueList) {
		return nil, errors.New("out of range")
	}
	d.readPos++
	return d.valueList[d.readPos-1], nil
}

func (d *dictStore) numValues() int32 {
	return int32(len(d.valueList))
}

func (d *dictStore) nullValueCount() int32 {
	return d.nullCount
}

func (d *dictStore) distinctValueCount() int64 {
	return int64(len(d.uniqueValues))
}

func (d *dictStore) sizes() (dictLen int64, noDictLen int64) {
	return d.uniqueValuesSize + int64(4*len(d.valueList)), d.allValuesSize
}

type dictEncoder struct {
	w        io.Writer
	bitWidth int
	indices  []int32
}

func (d *dictEncoder) Close() error {
	// first write the bitLength in a byte
	if err := writeFull(d.w, []byte{byte(d.bitWidth)}); err != nil {
		return err
	}
	enc := newHybridEncoder(d.bitWidth)
	if err := enc.init(d.w); err != nil {
		return err
	}
	if err := enc.encode(d.indices); err != nil {
		return err
	}

	return enc.Close()
}

func (d *dictEncoder) init(w io.Writer, bitWidth int) error {
	d.w = w
	d.bitWidth = bitWidth

	return nil
}

func (d *dictEncoder) encodeIndices(indices []int32) error {
	d.indices = append(d.indices, indices...)
	return nil
}
