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

// TODO:  Not sure about storing nil value here, see if we need to discard them?
type dictStore struct {
	values []interface{}
	data   []int32
}

func (d *dictStore) init() {
	d.values = d.values[:0]
	d.data = d.data[:0]
}

func (d *dictStore) getValues() []interface{} {
	return d.values
}

func (d *dictStore) getIndexes() []int32 {
	return d.data
}

func (d *dictStore) assemble() []interface{} {
	ret := make([]interface{}, len(d.data))
	for i := range d.data {
		ret[i] = d.values[d.data[i]]
	}

	return ret
}

func (d *dictStore) getIndex(in interface{}) int32 {
	for i := range d.values {
		// TODO: Better compare?
		if compare(d.values[i], in) {
			return int32(i)
		}
	}
	d.values = append(d.values, in)

	return int32(len(d.values) - 1)
}

func (d *dictStore) addValue(v interface{}) {
	d.data = append(d.data, d.getIndex(v))
}

// TODO: Implement fallback
type dictEncoder struct {
	w io.Writer
	dictStore
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
		d.addValue(values[i])
	}

	return nil
}
