package go_parquet

import (
	"io"

	"github.com/pkg/errors"
)

type dictDecoder struct {
	values []interface{}

	keys *hybridDecoder
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
	if w != 0 {
		d.keys = newHybridDecoder(w)
		return d.keys.init(r)
	}

	if len(d.values) > 0 {
		return errors.New("bit width zero with non-empty dictionary")
	}

	return nil
}

func (d *dictDecoder) decodeValues(dst []interface{}) error {
	if d.keys == nil {
		return errors.New("no value is inside dictionary")
	}
	size := int32(len(d.values))
	for i := range dst {
		key, err := d.keys.next()
		if err != nil {
			return err
		}

		if key > size {
			return errors.Errorf("dict: invalid index %d, values count are %d", key, size)
		}

		dst[i] = d.values[key]
	}

	return nil
}
