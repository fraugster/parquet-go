package go_parquet

import "io"

type booleanED interface {
	decodeBoolean(io.Reader, []bool) error
	encodeBoolean(io.Writer, []bool) error
}

type booleanPlainED struct {
	i      uint8
	values [8]int32
}

func (ed *booleanPlainED) decode(r io.Reader, dst interface{}) error {
	switch dst := dst.(type) {
	case []bool:
		return ed.decodeBoolean(r, dst)
	case []interface{}:
		b := make([]bool, len(dst))
		err := ed.decodeBoolean(r, b)
		for i := 0; i < len(dst); i++ {
			dst[i] = b[i]
		}
		return err
	default:
		panic("invalid argument")
	}
}

func (ed *booleanPlainED) encode(w io.Writer, dst interface{}) error {
	switch dst := dst.(type) {
	case []bool:
		return ed.encodeBoolean(w, dst)
		// TODO : Support other types
	default:
		panic("invalid argument")
	}
}

func (ed *booleanPlainED) decodeBoolean(r io.Reader, dst []bool) error {
	for i := 0; i < len(dst); i++ {
		if ed.i == 0 {
			buf := make([]byte, 1)
			if _, err := io.ReadFull(r, buf); err != nil {
				return err
			}
			ed.values = unpack8int32_1(buf)
		}
		dst[i] = ed.values[ed.i] == 1
		ed.i = (ed.i + 1) % 8
	}

	return nil
}

func (ed *booleanPlainED) encodeBoolean(w io.Writer, src []bool) error {
	l := len(src)
	for i := 0; i < l; i += 8 {
		var block [8]int32
		for j := 0; j < 8; j++ {
			if i+j < l {
				if src[i+j] {
					block[j] = 1
				}
			}
		}
		buf := pack8int32_1(block)
		if err := writeFull(w, buf); err != nil {
			return err
		}
	}

	return nil
}
