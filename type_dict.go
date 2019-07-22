package go_parquet

import (
	"io"
	"math"
	"math/bits"

	"github.com/pkg/errors"
)

type dictDecoder struct {
	values []interface{}

	keys decoder
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

// TODO: Implement fallback
type dictEncoder struct {
	w      io.Writer
	values []interface{}

	data []int32
}

func (d *dictEncoder) getValues() []interface{} {
	return d.values
}

func (d *dictEncoder) getIndex(in interface{}) int32 {
	for i := range d.values {
		// TODO: Better compare?
		if d.values[i] == in {
			return int32(i)
		}
	}
	d.values = append(d.values, in)

	return int32(len(d.values) - 1)
}

func (d *dictEncoder) Close() error {
	v := len(d.values)
	if v == 0 { // empty dictionary?
		return nil
	}

	// fallback to plain
	if v >= math.MaxInt16 {
		panic("TODO")
	}

	w := bits.Len(uint(v))
	// first write the bitLenght in a byte
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
	d.values = nil
	return nil
}

func (d *dictEncoder) encodeValues(values []interface{}) error {
	if d.data == nil {
		d.data = make([]int32, 0, len(values))
	}
	for i := range values {
		idx := d.getIndex(values[i])
		d.data = append(d.data, idx)
	}

	return nil
}
