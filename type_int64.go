package goparquet

type int64DeltaBPDecoder struct {
	deltaBitPackDecoder64
}

func (d *int64DeltaBPDecoder) decodeValues(dst []interface{}) (int, error) {
	for i := range dst {
		u, err := d.next()
		if err != nil {
			return i, err
		}
		dst[i] = u
	}

	return len(dst), nil
}

type int64DeltaBPEncoder struct {
	deltaBitPackEncoder64
}

func (d *int64DeltaBPEncoder) encodeValues(values []interface{}) error {
	for i := range values {
		if err := d.addInt64(values[i].(int64)); err != nil {
			return err
		}
	}

	return nil
}
