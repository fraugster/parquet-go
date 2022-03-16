package goparquet

import (
	"errors"
	"fmt"
	"io"
)

type deltaBitPackDecoder[T intType, I internalIntType[T]] struct {
	impl I

	r io.Reader

	blockSize           int32
	miniBlockCount      int32
	valuesCount         int32
	miniBlockValueCount int32

	previousValue T
	minDelta      T

	miniBlockBitWidth        []uint8
	currentMiniBlock         int32
	currentMiniBlockBitWidth uint8
	miniBlockPosition        int32 // position inside the current mini block
	position                 int32 // position in the value. since delta may have padding we need to track this
	currentUnpacker          func([]byte) [8]T
	miniBlock                [8]T
}

func (d *deltaBitPackDecoder[T, I]) initSize(r io.Reader) error {
	return d.init(r)
}

func (d *deltaBitPackDecoder[T, I]) init(r io.Reader) error {
	d.r = r

	if err := d.readBlockHeader(); err != nil {
		return err
	}

	if err := d.readMiniBlockHeader(); err != nil {
		return err
	}

	return nil
}

func (d *deltaBitPackDecoder[T, I]) readBlockHeader() error {
	var err error
	if d.blockSize, err = readUVariant32(d.r); err != nil {
		return fmt.Errorf("failed to read block size: %w", err)
	}
	if d.blockSize <= 0 && d.blockSize%128 != 0 {
		return errors.New("invalid block size")
	}

	if d.miniBlockCount, err = readUVariant32(d.r); err != nil {
		return fmt.Errorf("failed to read number of mini blocks: %w", err)
	}

	if d.miniBlockCount <= 0 || d.blockSize%d.miniBlockCount != 0 {
		return errors.New("int/delta: invalid number of mini blocks")
	}

	d.miniBlockValueCount = d.blockSize / d.miniBlockCount
	if d.miniBlockValueCount == 0 {
		return errors.New("invalid mini block value count, it can't be zero")
	}

	if d.valuesCount, err = readUVariant32(d.r); err != nil {
		return fmt.Errorf("failed to read total value count: %w", err)
	}

	if d.valuesCount < 0 {
		return errors.New("invalid total value count")
	}

	if d.previousValue, err = readVarint[T, I](d.r); err != nil {
		return fmt.Errorf("failed to read first value: %w", err)
	}

	return nil
}

func (d *deltaBitPackDecoder[T, I]) readMiniBlockHeader() error {
	var err error

	bitWidth := uint8(d.impl.Sizeof() * 8)

	if d.minDelta, err = readVarint[T, I](d.r); err != nil {
		return fmt.Errorf("failed to read min delta: %w", err)
	}

	// the mini block bitwidth is always there, even if the value is zero
	d.miniBlockBitWidth = make([]uint8, d.miniBlockCount)
	if _, err = io.ReadFull(d.r, d.miniBlockBitWidth); err != nil {
		return fmt.Errorf("not enough data to read all miniblock bit widths: %w", err)
	}

	for i := range d.miniBlockBitWidth {
		if d.miniBlockBitWidth[i] > bitWidth {
			return fmt.Errorf("invalid miniblock bit width : %d", d.miniBlockBitWidth[i])
		}
	}

	// start from the first min block in a big block
	d.currentMiniBlock = 0

	return nil
}

func (d *deltaBitPackDecoder[T, I]) next() (T, error) {
	if d.position >= d.valuesCount {
		// No value left in the buffer
		return 0, io.EOF
	}

	// need new byte?
	if d.position%8 == 0 {
		// do we need to advance a mini block?
		if d.position%d.miniBlockValueCount == 0 {
			// do we need to advance a big block?
			if d.currentMiniBlock >= d.miniBlockCount {
				if err := d.readMiniBlockHeader(); err != nil {
					return 0, err
				}
			}

			d.currentMiniBlockBitWidth = d.miniBlockBitWidth[d.currentMiniBlock]
			d.currentUnpacker = d.impl.GetUnpacker(int(d.currentMiniBlockBitWidth))

			d.miniBlockPosition = 0
			d.currentMiniBlock++
		}

		// read next 8 values
		w := int32(d.currentMiniBlockBitWidth)
		buf := make([]byte, w)
		if _, err := io.ReadFull(d.r, buf); err != nil {
			return 0, err
		}

		d.miniBlock = d.currentUnpacker(buf)
		d.miniBlockPosition += w
		// there is padding here, read them all from the reader, first deal with the remaining of the current block,
		// then the next blocks. if the blocks bit width is zero then simply ignore them, but the docs said reader
		// should accept any arbitrary bit width here.
		if d.position+8 >= d.valuesCount {
			//  current block
			l := (d.miniBlockValueCount/8)*w - d.miniBlockPosition
			if l < 0 {
				return 0, errors.New("invalid stream")
			}
			remaining := make([]byte, l)
			_, _ = io.ReadFull(d.r, remaining)
			for i := d.currentMiniBlock; i < d.miniBlockCount; i++ {
				w := int32(d.miniBlockBitWidth[d.currentMiniBlock])
				if w != 0 {
					remaining := make([]byte, (d.miniBlockValueCount/8)*w)
					_, _ = io.ReadFull(d.r, remaining)
				}
			}
		}
	}

	// value is the previous value + delta stored in the reader and the min delta for the block, also we always read one
	// value ahead
	ret := d.previousValue
	d.previousValue += d.miniBlock[d.position%8] + d.minDelta
	d.position++

	return ret, nil
}

func (d *deltaBitPackDecoder[T, I]) decodeValues(dst []interface{}) (int, error) {
	for i := range dst {
		u, err := d.next()
		if err != nil {
			return i, err
		}
		dst[i] = u
	}

	return len(dst), nil
}
