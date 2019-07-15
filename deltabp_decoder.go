package go_parquet

import (
	"io"

	"github.com/pkg/errors"
)

type int32DeltaBPDecoder struct {
	r io.Reader

	numMiniBlocks int32
	miniBlockSize int32
	numValues     int32

	minDelta        int32
	miniBlockWidths []uint8

	position        int
	value           int32
	miniBlock       int
	miniBlockWidth  int
	unpacker        unpack8int32Func
	miniBlockPos    int
	miniBlockValues [8]int32
}

func (d *int32DeltaBPDecoder) init(r io.Reader) error {
	d.r = r

	if err := d.readPageHeader(); err != nil {
		return err
	}
	if err := d.readBlockHeader(); err != nil {
		return err
	}

	return nil
}

// page-header := <block size in values> <number of miniblocks in a block> <total value count> <first value>
func (d *int32DeltaBPDecoder) readPageHeader() error {
	blockSize, err := readUVariant32(d.r)
	if err != nil {
		return errors.Wrap(err, "int32/delta: failed to read block size")
	}
	if blockSize <= 0 {
		// TODO: maybe validate blockSize % 8 = 0
		return errors.New("int32/delta: invalid block size")
	}

	d.numMiniBlocks, err = readUVariant32(d.r)
	if err != nil {
		return errors.Wrap(err, "int32/delta: failed to read number of mini blocks")
	}

	if d.numMiniBlocks <= 0 || d.numMiniBlocks > blockSize || blockSize%d.numMiniBlocks != 0 {
		// TODO: maybe blockSize/8 % d.numMiniBlocks = 0
		return errors.New("int32/delta: invalid number of mini blocks")
	}

	d.numValues, err = readUVariant32(d.r)
	if err != nil {
		return errors.Wrapf(err, "int32/delta: failed to read total value count")
	}
	if d.numValues < 0 {
		return errors.New("int32/delta: invalid total value count")
	}

	d.value, err = readVariant32(d.r)
	if err != nil {
		return errors.Wrap(err, "int32/delta: failed to read first value")
	}

	// TODO: re-use if possible
	d.miniBlockWidths = make([]byte, d.numMiniBlocks)
	d.miniBlockSize = blockSize / d.numMiniBlocks

	return nil
}

// block := <min delta> <list of bitwidths of miniblocks> <miniblocks>
// min delta : zig-zag var int encoded
// bitWidthsOfMiniBlock : 1 byte little endian
func (d *int32DeltaBPDecoder) readBlockHeader() error {
	var err error

	d.minDelta, err = readVariant32(d.r)
	if err != nil {
		return errors.Wrap(err, "int32/delta: failed to read min delta")
	}

	if _, err = io.ReadFull(d.r, d.miniBlockWidths); err != nil {
		return errors.Wrap(err, "int32/delta: not enough data to read all miniblock bit widths")
	}

	for i := range d.miniBlockWidths {
		if d.miniBlockWidths[i] < 0 || d.miniBlockWidths[i] > 32 {
			return errors.New("int32/delta: invalid miniblock bit width")
		}
	}

	d.miniBlock = 0

	return nil
}

func (d *int32DeltaBPDecoder) next() (int32, error) {
	if d.position >= int(d.numValues) {
		return 0, io.EOF
	}

	if d.position%8 == 0 {
		if d.position%int(d.miniBlockSize) == 0 {
			if d.miniBlock >= int(d.numMiniBlocks) {
				if err := d.readBlockHeader(); err != nil {
					return 0, err
				}
			}

			d.miniBlockWidth = int(d.miniBlockWidths[d.miniBlock])
			d.unpacker = unpack8Int32FuncByWidth[d.miniBlockWidth]
			d.miniBlockPos = 0
			d.miniBlock++
		}

		// read next 8 values
		w := d.miniBlockWidth
		buf := make([]byte, d.miniBlockWidth)
		_, err := io.ReadFull(d.r, buf)
		if err != nil {
			return 0, err
		}

		d.miniBlockValues = d.unpacker(buf)
		d.miniBlockPos += w
		if d.position+8 >= int(d.numValues) {
			buf := make([]byte, int(d.miniBlockSize)/8*w-d.miniBlockPos)
			// make sure that all data is consumed
			// this is needed for byte array decoders
			// TODO: (f0rud) What is this code here?
			_, _ = d.r.Read(buf)
		}
	}
	ret := d.value
	d.value += d.miniBlockValues[d.position%8] + d.minDelta
	d.position++

	return ret, nil
}
