package go_parquet

import (
	"io"
	"math/bits"

	"github.com/pkg/errors"
)

type dictDecoder struct {
	values []interface{}

	keys decoder
}

func (d *dictDecoder) bytesArray() {
	panic("should not call me")
}

func (d *dictDecoder) setValues(v []interface{}) {
	d.values = v
}

// the value should be there before the init
func (d *dictDecoder) init(r io.Reader) error {
	buf := make([]byte, 1)
	if _, err := io.ReadFull(r, buf); err != nil {
		return err
	}
	w := int(buf[0])
	if w < 0 || w > 32 {
		return errors.Errorf("invalid bitwidth %d", w)
	}
	if w >= 0 {
		d.keys = newHybridDecoder(w)
		return d.keys.init(r)
	}

	return errors.New("bit width zero with non-empty dictionary")
}

func (d *dictDecoder) decodeValues(dst []interface{}) (int, error) {
	if d.keys == nil {
		return 0, errors.New("no value is inside dictionary")
	}
	size := int32(len(d.values))

	for i := range dst {
		key, err := d.keys.next()
		if err != nil {
			return i, err
		}

		if key >= size {
			return 0, errors.Errorf("dict: invalid index %d, values count are %d", key, size)
		}

		dst[i] = d.values[key]
	}

	return len(dst), nil
}

type dictStore struct {
	values    []interface{}
	data      []int32
	nullCount int32
	size      int64
	valueSize int64
}

func (d *dictStore) init() {
	d.values = d.values[:0]
	d.data = d.data[:0]
	d.nullCount = 0
}

func (d *dictStore) getValues() []interface{} {
	return d.values
}

func (d *dictStore) getIndexes() []int32 {
	return d.data
}

func (d *dictStore) assemble(null bool) []interface{} {
	ret := make([]interface{}, 0, len(d.data))
	for i := range d.data {
		if d.data[i] < 0 {
			if null {
				ret = append(ret, nil)
			}
			continue
		}
		ret = append(ret, d.values[d.data[i]])
	}

	return ret
}

func (d *dictStore) getIndex(in interface{}, size int) int32 {
	for i := range d.values {
		// TODO: Better compare?
		if compare(d.values[i], in) {
			return int32(i)
		}
	}
	d.valueSize += int64(size)
	d.values = append(d.values, in)

	return int32(len(d.values) - 1)
}

func (d *dictStore) addValue(v interface{}, size int) {
	if v == nil {
		d.nullCount++
		d.data = append(d.data, -1)
		return
	}
	d.size += int64(size)
	d.data = append(d.data, d.getIndex(v, size))
}

func (d *dictStore) numValues() int32 {
	return int32(len(d.data))
}

func (d *dictStore) numDistinctValues() int32 {
	return int32(len(d.values))
}

func (d *dictStore) numNullValue() int32 {
	return d.nullCount
}

// sizes is an experimental guess for the dictionary size and real value size (when there is no dictionary)
func (d *dictStore) sizes() (dictLen int64, noDictLen int64) {
	count := len(d.data) - int(d.nullCount)
	max := bits.Len(uint(len(d.values))) // bits required for any value in data
	if max > 0 {
		dictLen = int64(count/max) + 1
	}

	dictLen += d.valueSize
	noDictLen = d.size
	return

}

// TODO: Implement fallback
type dictEncoder struct {
	w io.Writer
	dictStore
}

func (d *dictEncoder) bytesArray() {
	panic("do not call me")
}

func (d *dictEncoder) Close() error {
	v := len(d.values)
	if v == 0 { // empty dictionary?
		return errors.New("empty dictionary nothing to write")
	}

	w := bits.Len(uint(v))
	// first write the bitLength in a byte
	if err := writeFull(d.w, []byte{byte(w)}); err != nil {
		return err
	}
	enc := newHybridEncoder(w)
	if err := enc.init(d.w); err != nil {
		return err
	}
	if err := enc.encode(d.data); err != nil {
		return err
	}

	return enc.Close()
}

func (d *dictEncoder) init(w io.Writer) error {
	d.w = w
	d.dictStore.init()

	return nil
}

func (d *dictEncoder) encodeValues(values []interface{}) error {
	for i := range values {
		d.addValue(values[i], 0) // size is not important here
	}

	return nil
}
