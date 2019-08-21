package go_parquet

import (
	"bytes"
	"io"

	"github.com/pkg/errors"
	"github.com/fraugster/parquet-go/parquet"
)

type dataPageReaderV1 struct {
	ph *parquet.PageHeader

	valuesCount        int32
	encoding           parquet.Encoding
	dDecoder, rDecoder levelDecoder
	valuesDecoder      valuesDecoder
	fn                 getValueDecoderFn

	position int
}

func (dp *dataPageReaderV1) numValues() int32 {
	return dp.valuesCount
}

func (dp *dataPageReaderV1) readValues(val []interface{}) (n int, dLevel []int32, rLevel []int32, err error) {
	size := len(val)
	if rem := int(dp.valuesCount) - dp.position; rem < size {
		size = rem
	}

	if size == 0 {
		return 0, nil, nil, nil
	}

	dLevel = make([]int32, size)
	if err := decodeInt32(dp.dDecoder, dLevel); err != nil {
		return 0, nil, nil, errors.Wrap(err, "read definition levels failed")
	}

	rLevel = make([]int32, size)
	if err := decodeInt32(dp.rDecoder, rLevel); err != nil {
		return 0, nil, nil, errors.Wrap(err, "read repetition levels failed")
	}

	notNull := 0
	for _, dl := range dLevel {
		if dl == int32(dp.dDecoder.maxLevel()) {
			notNull++
		}
	}

	if notNull != 0 {
		if n, err := dp.valuesDecoder.decodeValues(val[:notNull]); err != nil {
			return 0, nil, nil, errors.Wrapf(err, "read values from page failed, need %d value read %d", notNull, n)
		}
	}
	dp.position += size
	return size, dLevel, rLevel, nil
}

func (dp *dataPageReaderV1) init(dDecoder, rDecoder getLevelDecoder, values getValueDecoderFn) error {
	var err error
	dp.dDecoder, err = dDecoder(dp.ph.DataPageHeader.DefinitionLevelEncoding)
	if err != nil {
		return err
	}
	dp.rDecoder, err = rDecoder(dp.ph.DataPageHeader.RepetitionLevelEncoding)
	if err != nil {
		return err
	}

	dp.fn = values
	dp.position = 0

	return nil
}

func (dp *dataPageReaderV1) read(r io.ReadSeeker, ph *parquet.PageHeader, codec parquet.CompressionCodec) (err error) {
	if ph.DataPageHeader == nil {
		return errors.Errorf("null DataPageHeader in %+v", ph)
	}

	if dp.valuesCount = ph.DataPageHeader.NumValues; dp.valuesCount < 0 {
		return errors.Errorf("negative NumValues in DATA_PAGE: %d", dp.valuesCount)
	}
	reader, err := createDataReader(r, codec, ph.GetCompressedPageSize(), ph.GetUncompressedPageSize())
	if err != nil {
		return err
	}

	dp.encoding = ph.DataPageHeader.Encoding
	dp.ph = ph

	if dp.valuesDecoder, err = dp.fn(dp.encoding); err != nil {
		return err
	}

	if err := dp.dDecoder.initSize(reader); err != nil {
		return err
	}

	if err := dp.rDecoder.initSize(reader); err != nil {
		return err
	}

	return dp.valuesDecoder.init(reader)
}

type dataPageWriterV1 struct {
	col *column

	codec      parquet.CompressionCodec
	dictionary bool
}

func (dp *dataPageWriterV1) init(schema SchemaWriter, col *column, codec parquet.CompressionCodec) error {
	dp.col = col
	dp.codec = codec
	return nil
}

func (dp *dataPageWriterV1) getHeader(comp, unComp int) *parquet.PageHeader {
	enc := dp.col.data.encoding()
	if dp.dictionary {
		enc = parquet.Encoding_RLE_DICTIONARY
	}
	ph := &parquet.PageHeader{
		Type:                 parquet.PageType_DATA_PAGE,
		UncompressedPageSize: int32(unComp),
		CompressedPageSize:   int32(comp),
		Crc:                  nil, // TODO: add crc?
		DataPageHeader: &parquet.DataPageHeader{
			NumValues: dp.col.data.values.numValues() + dp.col.data.values.nullValueCount(),
			Encoding:  enc,
			// Only RLE supported for now, not sure if we need support for more encoding
			DefinitionLevelEncoding: parquet.Encoding_RLE,
			RepetitionLevelEncoding: parquet.Encoding_RLE,
			// TODO : add statistics support
			Statistics: nil,
		},
	}
	return ph
}

func (dp *dataPageWriterV1) write(w io.Writer) (int, int, error) {
	dataBuf := &bytes.Buffer{}
	// Only write definition value higher than zero
	if dp.col.MaxDefinitionLevel() > 0 {
		if err := encodeLevels(dataBuf, dp.col.MaxDefinitionLevel(), dp.col.data.dLevels); err != nil {
			return 0, 0, err
		}
	}
	// Only write repetition value higher than zero
	if dp.col.MaxRepetitionLevel() > 0 {
		if err := encodeLevels(dataBuf, dp.col.MaxRepetitionLevel(), dp.col.data.rLevels); err != nil {
			return 0, 0, err
		}
	}

	enc := dp.col.data.encoding()
	if dp.dictionary {
		enc = parquet.Encoding_RLE_DICTIONARY
	}

	encoder, err := getValuesEncoder(enc, dp.col.Element(), dp.col.data.values)
	if err != nil {
		return 0, 0, err
	}

	// TODO: there is a redundant loop and copy if the value encoder is a dictEncoder
	if err := encodeValue(dataBuf, encoder, dp.col.data.values.assemble()); err != nil {
		return 0, 0, err
	}

	comp, err := compressBlock(dataBuf.Bytes(), dp.codec)
	if err != nil {
		return 0, 0, errors.Wrapf(err, "compressing data failed with %s method", dp.codec)
	}
	compSize, unCompSize := len(comp), len(dataBuf.Bytes())

	header := dp.getHeader(compSize, unCompSize)
	if err := writeThrift(header, w); err != nil {
		return 0, 0, err
	}

	return compSize, unCompSize, writeFull(w, comp)
}
