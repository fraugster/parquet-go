package go_parquet

import (
	"bytes"
	"io"
	"strings"

	"github.com/pkg/errors"
	"github.com/fraugster/parquet-go/parquet"
)

type dataPageReaderV1 struct {
	ph *parquet.PageHeader

	numValues          int32
	encoding           parquet.Encoding
	dDecoder, rDecoder levelDecoder
	valuesDecoder      valuesDecoder
	fn                 getValueDecoderFn

	position int
}

func (dp *dataPageReaderV1) readValues(val []interface{}) (n int, dLevel []uint16, rLevel []uint16, err error) {
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

	if dp.numValues = ph.DataPageHeader.NumValues; dp.numValues < 0 {
		return errors.Errorf("negative NumValues in DATA_PAGE: %d", dp.numValues)
	}
	dp.encoding = ph.DataPageHeader.Encoding
	dp.ph = ph

	if dp.valuesDecoder, err = dp.fn(dp.encoding); err != nil {
		return err
	}

	if err := dp.dDecoder.initSize(r); err != nil {
		return err
	}

	if err := dp.rDecoder.initSize(r); err != nil {
		return err
	}

	reader, err := createDataReader(r, codec, ph.GetCompressedPageSize(), ph.GetUncompressedPageSize())
	if err != nil {
		return err
	}

	return dp.valuesDecoder.init(reader)
}

type dataPageWriterV1 struct {
	col *column

	codec      parquet.CompressionCodec
	dictionary bool
}

func (dp *dataPageWriterV1) init(schema *Schema, col *column, codec parquet.CompressionCodec) error {
	dp.col = col
	dp.codec = codec
	return nil
}

func (dp *dataPageWriterV1) getHeader(comp, unComp int) *parquet.PageHeader {
	ph := &parquet.PageHeader{
		Type:                 parquet.PageType_DATA_PAGE,
		UncompressedPageSize: int32(unComp),
		CompressedPageSize:   int32(comp),
		Crc:                  nil, // TODO: add crc?
		DataPageHeader: &parquet.DataPageHeader{
			NumValues: dp.col.data.dictionary().numValues(),
			Encoding:  dp.col.data.encoding(),
			// Only RLE supported for now, not sure if we need support for more encoding
			DefinitionLevelEncoding: parquet.Encoding_RLE,
			RepetitionLevelEncoding: parquet.Encoding_RLE,
			// TODO : add statistics support
			Statistics: nil,
		},
	}
	return ph
}

func (dp *dataPageWriterV1) Write(w io.Writer) (int, int, error) {
	// In V1 data page is compressed separately
	dataBuf := &bytes.Buffer{}
	nested := strings.IndexByte(dp.col.FlatName(), '.') >= 0
	// if it is nested or it is not repeated we need the dLevel data
	if nested || dp.col.data.repetitionType() != parquet.FieldRepetitionType_REQUIRED {
		if err := encodeLevels(dataBuf, dp.col.MaxDefinitionLevel(), dp.col.data.definitionLevels()); err != nil {
			return 0, 0, err
		}
	}
	// if this is nested or if the data is repeated
	if nested || dp.col.data.repetitionType() == parquet.FieldRepetitionType_REPEATED {
		if err := encodeLevels(dataBuf, dp.col.MaxRepetitionLevel(), dp.col.data.repetitionLevels()); err != nil {
			return 0, 0, err
		}
	}

	enc := dp.col.data.encoding()
	if dp.dictionary {
		enc = parquet.Encoding_RLE_DICTIONARY
	}

	encoder, err := getValuesEncoder(enc, dp.col.Element(), dp.col.data.dictionary())
	if err != nil {
		return 0, 0, err
	}

	if err := encodeValue(dataBuf, encoder, dp.col.data.dictionary().assemble(false)); err != nil {
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
