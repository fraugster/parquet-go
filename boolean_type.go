package go_parquet

import (
	"io"
)

type booleanPlainDecoder struct {
	r    io.Reader
	left []bool
}

// copy the left overs from the previous call. instead of returning an empty subset of the old slice,
// it delete the slice (by returning nil) so there is no memory leak because of the underlying array
// the return value is the new left over and the number of read message
func copyLeftOvers(dst []interface{}, src []bool) ([]bool, int) {
	size := len(dst)
	var clean bool
	if len(src) <= size {
		size = len(src)
		clean = true
	}

	for i := 0; i < size; i++ {
		dst[i] = src[i]
	}
	if clean {
		return nil, size
	}

	return src[size:], size
}

func (b *booleanPlainDecoder) init(r io.Reader) error {
	b.r = r
	b.left = nil

	return nil
}

func (b *booleanPlainDecoder) decodeValues(dst []interface{}) error {
	var start int
	if len(b.left) > 0 {
		// there is a leftover from the last run
		b.left, start = copyLeftOvers(dst, b.left)
		if b.left != nil {
			return nil
		}
	}

	buf := make([]byte, 1)
	for i := start; i < len(dst); i += 8 {
		if _, err := io.ReadFull(b.r, buf); err != nil {
			return err
		}
		d := unpack8int32_1(buf)
		for j := 0; j < 8; j++ {
			if i+j < len(dst) {
				dst[i+j] = d[j] == 1
			} else {
				b.left = append(b.left, d[j] == 1)
			}
		}
	}

	return nil
}

type booleanRLEDecoder struct {
	decoder *hybridDecoder
}

func (b *booleanRLEDecoder) init(r io.Reader) error {
	b.decoder = newHybridDecoder(1)
	return b.decoder.init(r)
}

func (b *booleanRLEDecoder) decodeValues(dst []interface{}) error {
	for i := 0; i < len(dst); i += 1 {
		n, err := b.decoder.next()
		if err != nil {
			return err
		}
		dst[i] = n == 1
	}

	return nil
}
