package go_parquet

import (
	"bytes"
	"io"

	"github.com/pkg/errors"
	"github.com/fraugster/parquet-go/parquet"
)

type dataPageReaderV2 struct {
	ph *parquet.PageHeader

	numValues          int32
	encoding           parquet.Encoding
	valuesDecoder      valuesDecoder
	dDecoder, rDecoder levelDecoder
	fn                 getValueDecoderFn
	position           int
}

func (dp *dataPageReaderV2) readValues(val []interface{}) (n int, dLevel []uint16, rLevel []uint16, err error) {
	size := len(val)
	if rem := int(dp.numValues) - dp.position; rem < size {
		size = rem
	}

	if size == 0 {
		return 0, nil, nil, nil
	}

	dLevel = make([]uint16, size)
	if err := decodeUint16(dp.dDecoder, dLevel); err != nil {
		return 0, nil, nil, errors.Wrap(err, "read definition levels failed")
	}

	rLevel = make([]uint16, size)
	if err := decodeUint16(dp.rDecoder, rLevel); err != nil {
		return 0, nil, nil, errors.Wrap(err, "read repetition levels failed")
	}

	notNull := 0
	for _, dl := range dLevel {
		if dl == dp.dDecoder.maxLevel() {
			notNull++
		}
	}

	if notNull != 0 {
		if n, err := dp.valuesDecoder.decodeValues(val[:notNull]); err != nil {
			return 0, nil, nil, errors.Wrapf(err, "read values from page failed, need %d values but read %d", notNull, n)
		}
	}
	dp.position += size
	return size, dLevel, rLevel, nil
}

func (dp *dataPageReaderV2) init(dDecoder, rDecoder getLevelDecoder, values getValueDecoderFn) error {
	var err error
	// Page v2 dose not have any encoding for the levels
	dp.dDecoder, err = dDecoder(parquet.Encoding_RLE)
	if err != nil {
		return err
	}
	dp.rDecoder, err = rDecoder(parquet.Encoding_RLE)
	if err != nil {
		return err
	}
	dp.fn = values
	dp.position = 0

	return nil
}

func (dp *dataPageReaderV2) read(r io.ReadSeeker, ph *parquet.PageHeader, codec parquet.CompressionCodec) (err error) {
	// TODO: verify this format, there is some question
	// 1- Uncompressed size is affected by the level lens?
	// 2- If the levels are actually rle and the first byte is the size, since there is already size in header (NO)
	if ph.DataPageHeaderV2 == nil {
		return errors.Errorf("null DataPageHeaderV2 in %+v", ph)
	}

	if dp.numValues = ph.DataPageHeaderV2.NumValues; dp.numValues < 0 {
		return errors.Errorf("negative NumValues in DATA_PAGE_V2: %d", dp.numValues)
	}

	if ph.DataPageHeaderV2.RepetitionLevelsByteLength < 0 {
		return errors.Errorf("invalid RepetitionLevelsByteLength")
	}
	if ph.DataPageHeaderV2.DefinitionLevelsByteLength < 0 {
		return errors.Errorf("invalid DefinitionLevelsByteLength")
	}
	dp.encoding = ph.DataPageHeader.Encoding
	dp.ph = ph

	if dp.valuesDecoder, err = dp.fn(dp.encoding); err != nil {
		return err
	}

	// Its safe to call this {r,d}Decoder later, since the stream they operate on are in memory
	levelsSize := ph.DataPageHeaderV2.RepetitionLevelsByteLength + ph.DataPageHeaderV2.DefinitionLevelsByteLength
	// read both level size
	if levelsSize > 0 {
		data := make([]byte, levelsSize)
		n, err := io.ReadFull(r, data)
		if err != nil {
			return errors.Wrapf(err, "need to read %d byte but there was only %d byte", levelsSize, n)
		}
		if ph.DataPageHeaderV2.RepetitionLevelsByteLength > 0 {
			if err := dp.rDecoder.init(bytes.NewReader(data[:int(ph.DataPageHeaderV2.RepetitionLevelsByteLength)])); err != nil {
				return errors.Wrapf(err, "read repetition level failed")
			}
		}
		if ph.DataPageHeaderV2.DefinitionLevelsByteLength > 0 {
			if err := dp.dDecoder.init(bytes.NewReader(data[int(ph.DataPageHeaderV2.RepetitionLevelsByteLength):])); err != nil {
				return errors.Wrapf(err, "read definition level failed")
			}
		}
	}

	// TODO: (F0rud) I am not sure if this is correct to subtract the level size from the compressed size here
	reader, err := createDataReader(r, codec, ph.GetCompressedPageSize()-levelsSize, ph.GetUncompressedPageSize()-levelsSize)
	if err != nil {
		return err
	}

	return dp.valuesDecoder.init(reader)
}
