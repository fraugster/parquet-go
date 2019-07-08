package go_parquet

import (
	"errors"
	"io"
)

// bitpackRead try to read the bitpacked value into array of int
// TODO(F0ruD): Make sure we can use int32, and not interface here
func bitpackRead(r io.Reader, bitWidth int, blockCount int) ([]int32, error) {
	buf := make([]byte, bitWidth)
	res := make([]int32, 0, blockCount*8)

	// bitWidth should be between 0 and 32
	fn := unpack8Int32FuncByWidth[bitWidth]

	for i := 0; i < blockCount; i++ {
		n, err := r.Read(buf)
		if err != nil {
			return nil, err
		}
		if n != bitWidth {
			return nil, io.ErrUnexpectedEOF
		}
		data := fn(buf)
		res = append(res, data[0], data[1], data[2], data[3], data[4], data[5], data[6], data[7])
	}

	return res, nil
}

func bitpackWrite(w io.Writer, bitWidth int, data []int32) error {
	if len(data)%8 != 0 {
		return errors.New("data count should be some thing multiply 8")
	}
	blockCount := len(data) / 8
	fn := pack8Int32FuncByWidth[bitWidth]

	for i := 0; i < blockCount; i++ {
		b := i * 8
		buf := [...]int32{
			data[b], data[b+1], data[b+2], data[b+3], data[b+4], data[b+5], data[b+6], data[b+7],
		}
		n, err := w.Write(fn(buf))
		if err != nil {
			return err
		}

		if n != len(buf) {
			return errors.New("couldn't write entire buffer into the writer")
		}
	}

	return nil
}
