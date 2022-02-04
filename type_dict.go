package goparquet

import (
	"io"
	"math/bits"

	"github.com/pkg/errors"
)

type dictDecoder struct {
	values []interface{}

	keys decoder
}

// just for tests
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
		err := d.keys.init(r)
		return err
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

		if key < 0 || key >= size {
			return 0, errors.Errorf("dict: invalid index %d, values count are %d", key, size)
		}

		dst[i] = d.values[key]
	}

	return len(dst), nil
}

type dictStore struct {
	values     []interface{}
	data       []int32
	indices    map[interface{}]int32
	size       int64
	valueSize  int64
	keySize    int64
	readPos    int
	nullCount  int32
	noDictMode bool
}

func (d *dictStore) init() {
	d.indices = make(map[interface{}]int32)
	d.values = nil
	d.data = nil
	d.nullCount = 0
	d.readPos = 0
	d.size = 0
	d.valueSize = 0
}

func (d *dictStore) reset() {
	d.data = nil
	d.nullCount = 0
	d.readPos = 0
	d.size = 0
	d.valueSize = 0
}

func (d *dictStore) assemble() []interface{} {
	if d.noDictMode {
		return d.values
	}
	ret := make([]interface{}, 0, len(d.data))
	for i := range d.data {
		ret = append(ret, d.values[d.data[i]])
	}

	return ret
}

func (d *dictStore) getIndex(in interface{}, size int) int32 {
	key := mapKey(in)
	if idx, ok := d.indices[key]; ok {
		return idx
	}
	d.valueSize += int64(size)
	d.values = append(d.values, in)
	idx := int32(len(d.values) - 1)
	d.indices[key] = idx
	return idx
}

func (d *dictStore) addValue(v interface{}, size int) {
	if v == nil {
		d.nullCount++
		return
	}
	d.size += int64(size)
	if d.noDictMode {
		d.values = append(d.values, v)
		return
	}
	idx := d.getIndex(v, size)
	d.data = append(d.data, idx)
}

func (d *dictStore) getNextValue() (interface{}, error) {
	if d.noDictMode {
		if d.readPos >= len(d.values) {
			return nil, errors.New("out of range")
		}
		d.readPos++
		return d.values[d.readPos-1], nil
	}

	if d.readPos >= len(d.data) {
		return nil, errors.New("out of range")
	}
	d.readPos++
	pos := d.data[d.readPos-1]
	v := d.values[pos]
	return v, nil
}

func (d *dictStore) numValues() int32 {
	return int32(len(d.data))
}

func (d *dictStore) nullValueCount() int32 {
	return d.nullCount
}

func (d *dictStore) numDistinctValues() int32 {
	return int32(len(d.values))
}

func (d *dictStore) sizes() (dictLen int64, noDictLen int64) {
	return d.valueSize, d.size
}

type dictEncoder struct {
	w io.Writer
	*dictStore
}

func (d *dictEncoder) Close() error {
	v := len(d.values)

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
	//d.dictStore.init()

	return nil
}

func (d *dictEncoder) encodeValues(values []interface{}) error {
	// TODO: remove this function
	return nil
}

// just for tests
func (d *dictEncoder) getValues() []interface{} {
	return d.values
}
