package go_parquet

import (
	"encoding/binary"
	"io"

	"github.com/pkg/errors"
)

type hybridEncoder struct {
	w io.Writer

	bitWidth   int
	unpackerFn pack8int32Func
}

func newHybridEncoder(w io.Writer, bitWidth int) *hybridEncoder {
	return &hybridEncoder{
		w: w,

		bitWidth:   bitWidth,
		unpackerFn: pack8Int32FuncByWidth[bitWidth],
	}
}

func (he *hybridEncoder) write(items ...[]byte) error {
	for i := range items {
		l := len(items[i])
		n, err := he.w.Write(items[i])
		if err != nil {
			return err
		}

		if n != l {
			return errors.Errorf("need to write %d byte wrote %d", l, n)
		}
	}

	return nil
}

// TODO: this function is not use, since we are using the bp always.
func (he *hybridEncoder) rleEncode(count int, value int32) error {
	header := count << 1 // indicate this is a rle run

	buf := make([]byte, 4) // big enough for int
	cnt := binary.PutUvarint(buf, uint64(header))

	return he.write(buf[:cnt], encodeRLEValue(value, he.bitWidth))
}

func (he *hybridEncoder) bpEncode(data []int32) error {
	l := len(data)
	if l%8 != 0 {
		return errors.Errorf("bit-pack should be multiple of 8 values at a time, but it's %q", l)
	}

	res := make([]byte, 0, (l/8)*he.bitWidth)
	for i := 0; i < l; i += 8 {
		toW := [...]int32{
			data[i], data[i+1], data[i+2], data[i+3], data[i+4], data[i+5], data[i+6], data[i+7],
		}
		res = append(res, he.unpackerFn(toW)...)
	}

	header := ((l / 8) << 1) | 1
	buf := make([]byte, 4) // big enough for int
	cnt := binary.PutUvarint(buf, uint64(header))

	return he.write(buf[:cnt], res)
}

func (he *hybridEncoder) encode(data []int32) error {
	// TODO: Not sure how to decide on the bitpack or rle, so just bp is supported
	return he.bpEncode(data)
}
