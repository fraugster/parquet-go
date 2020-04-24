package goparquet

import (
	"bytes"
	"io"

	"github.com/fraugster/parquet-go/parquet"
	"github.com/pkg/errors"
)

type dataPageReaderV2 struct {
	ph *parquet.PageHeader

	valuesCount        int32
	encoding           parquet.Encoding
	valuesDecoder      valuesDecoder
	dDecoder, rDecoder levelDecoder
	fn                 getValueDecoderFn
	position           int
}

func (dp *dataPageReaderV2) numValues() int32 {
	return dp.valuesCount
}

func (dp *dataPageReaderV2) readValues(val []interface{}) (n int, dLevel *packedArray, rLevel *packedArray, err error) {
	size := len(val)
	if rem := int(dp.valuesCount) - dp.position; rem < size {
		size = rem
	}

	if size == 0 {
		return 0, nil, nil, nil
	}

	rLevel, _, err = decodePackedArray(dp.rDecoder, size)
	if err != nil {
		return 0, nil, nil, errors.Wrap(err, "read repetition levels failed")
	}

	var notNull int
	dLevel, notNull, err = decodePackedArray(dp.dDecoder, size)
	if err != nil {
		return 0, nil, nil, errors.Wrap(err, "read definition levels failed")
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

func (dp *dataPageReaderV2) read(r io.Reader, ph *parquet.PageHeader, codec parquet.CompressionCodec) error {
	// 1- Uncompressed size is affected by the level lens.
	// 2- In page V2 the rle size is in header, not in level stream
	if ph.DataPageHeaderV2 == nil {
		return errors.Errorf("null DataPageHeaderV2 in %+v", ph)
	}

	if dp.valuesCount = ph.DataPageHeaderV2.NumValues; dp.valuesCount < 0 {
		return errors.Errorf("negative NumValues in DATA_PAGE_V2: %d", dp.valuesCount)
	}

	if ph.DataPageHeaderV2.RepetitionLevelsByteLength < 0 {
		return errors.Errorf("invalid RepetitionLevelsByteLength")
	}
	if ph.DataPageHeaderV2.DefinitionLevelsByteLength < 0 {
		return errors.Errorf("invalid DefinitionLevelsByteLength")
	}
	dp.encoding = ph.DataPageHeaderV2.Encoding
	dp.ph = ph

	{ // to hide the govet shadow error
		var err error
		if dp.valuesDecoder, err = dp.fn(dp.encoding); err != nil {
			return err
		}
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

	reader, err := createDataReader(r, codec, ph.GetCompressedPageSize()-levelsSize, ph.GetUncompressedPageSize()-levelsSize)
	if err != nil {
		return err
	}

	return dp.valuesDecoder.init(reader)
}

type dataPageWriterV2 struct {
	col    *Column
	schema SchemaWriter

	codec      parquet.CompressionCodec
	dictionary bool
}

func (dp *dataPageWriterV2) init(schema SchemaWriter, col *Column, codec parquet.CompressionCodec) error {
	dp.col = col
	dp.codec = codec
	dp.schema = schema
	return nil
}

func (dp *dataPageWriterV2) getHeader(comp, unComp, defSize, repSize int, isCompressed bool) *parquet.PageHeader {
	enc := dp.col.data.encoding()
	if dp.dictionary {
		enc = parquet.Encoding_RLE_DICTIONARY
	}
	ph := &parquet.PageHeader{
		Type:                 parquet.PageType_DATA_PAGE_V2,
		UncompressedPageSize: int32(unComp + defSize + repSize),
		CompressedPageSize:   int32(comp + defSize + repSize),
		Crc:                  nil,
		DataPageHeaderV2: &parquet.DataPageHeaderV2{
			NumValues:                  dp.col.data.values.numValues() + dp.col.data.values.nullValueCount(),
			NumNulls:                   dp.col.data.values.nullValueCount(),
			NumRows:                    int32(dp.schema.rowGroupNumRecords()),
			Encoding:                   enc,
			DefinitionLevelsByteLength: int32(defSize),
			RepetitionLevelsByteLength: int32(repSize),
			IsCompressed:               isCompressed,
			Statistics:                 nil,
		},
	}
	return ph
}

func (dp *dataPageWriterV2) write(w io.Writer) (int, int, error) {
	rep := &bytes.Buffer{}

	// Only write repetition value higher than zero
	if dp.col.MaxRepetitionLevel() > 0 {
		if err := encodeLevelsV2(rep, dp.col.MaxRepetitionLevel(), dp.col.data.rLevels); err != nil {
			return 0, 0, err
		}
	}

	def := &bytes.Buffer{}

	// Only write definition level higher than zero
	if dp.col.MaxDefinitionLevel() > 0 {
		if err := encodeLevelsV2(def, dp.col.MaxDefinitionLevel(), dp.col.data.dLevels); err != nil {
			return 0, 0, err
		}
	}

	dataBuf := &bytes.Buffer{}
	enc := dp.col.data.encoding()
	if dp.dictionary {
		enc = parquet.Encoding_RLE_DICTIONARY
	}

	encoder, err := getValuesEncoder(enc, dp.col.Element(), dp.col.data.values)
	if err != nil {
		return 0, 0, err
	}

	if err = encodeValue(dataBuf, encoder, dp.col.data.values.assemble()); err != nil {
		return 0, 0, err
	}

	comp, err := compressBlock(dataBuf.Bytes(), dp.codec)
	if err != nil {
		return 0, 0, errors.Wrapf(err, "compressing data failed with %s method", dp.codec)
	}
	compSize, unCompSize := len(comp), len(dataBuf.Bytes())
	defLen, repLen := def.Len(), rep.Len()
	header := dp.getHeader(compSize, unCompSize, defLen, repLen, dp.codec != parquet.CompressionCodec_UNCOMPRESSED)
	if err := writeThrift(header, w); err != nil {
		return 0, 0, err
	}

	if err := writeFull(w, rep.Bytes()); err != nil {
		return 0, 0, err
	}

	if err := writeFull(w, def.Bytes()); err != nil {
		return 0, 0, err
	}

	return compSize + defLen + repLen, unCompSize + defLen + repLen, writeFull(w, comp)
}

func newDataPageV2Writer(useDict bool) pageWriter {
	return &dataPageWriterV2{
		dictionary: useDict,
	}
}
