package goparquet

type int32DeltaBPDecoder struct {
	deltaBitPackDecoder32
}

func (d *int32DeltaBPDecoder) decodeValues(dst []interface{}) (int, error) {
	for i := range dst {
		u, err := d.next()
		if err != nil {
			return i, err
		}
		dst[i] = u
	}

	return len(dst), nil
}

type int32DeltaBPEncoder struct {
	deltaBitPackEncoder32
}

func (d *int32DeltaBPEncoder) encodeValues(values []interface{}) error {
	for i := range values {
		if err := d.addInt32(values[i].(int32)); err != nil {
			return err
		}
	}

	return nil
}
