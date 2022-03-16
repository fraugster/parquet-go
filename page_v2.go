package goparquet

import (
	"bytes"
	"context"
	"fmt"
	"hash/crc32"
	"io"

	"github.com/fraugster/parquet-go/parquet"
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

func (dp *dataPageReaderV2) readValues(size int) (values []any, dLevel *packedArray, rLevel *packedArray, err error) {
	if rem := int(dp.valuesCount) - dp.position; rem < size {
		size = rem
	}

	if size == 0 {
		return nil, nil, nil, nil
	}

	rLevel, _, err = decodePackedArray(dp.rDecoder, size)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("read repetition levels failed: %w", err)
	}

	var notNull int
	dLevel, notNull, err = decodePackedArray(dp.dDecoder, size)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("read definition levels failed: %w", err)
	}

	val := make([]any, notNull)

	if notNull != 0 {
		if n, err := dp.valuesDecoder.decodeValues(val); err != nil {
			return nil, nil, nil, fmt.Errorf("read values from page failed, need %d values but read %d: %w", notNull, n, err)
		}
	}
	dp.position += size
	return val, dLevel, rLevel, nil
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

func (dp *dataPageReaderV2) read(r io.Reader, ph *parquet.PageHeader, codec parquet.CompressionCodec, validateCRC bool) error {
	// 1- Uncompressed size is affected by the level lens.
	// 2- In page V2 the rle size is in header, not in level stream
	if ph.DataPageHeaderV2 == nil {
		return fmt.Errorf("null DataPageHeaderV2 in %+v", ph)
	}

	if dp.valuesCount = ph.DataPageHeaderV2.NumValues; dp.valuesCount < 0 {
		return fmt.Errorf("negative NumValues in DATA_PAGE_V2: %d", dp.valuesCount)
	}

	if ph.DataPageHeaderV2.RepetitionLevelsByteLength < 0 {
		return fmt.Errorf("invalid RepetitionLevelsByteLength %d", ph.DataPageHeaderV2.RepetitionLevelsByteLength)
	}
	if ph.DataPageHeaderV2.DefinitionLevelsByteLength < 0 {
		return fmt.Errorf("invalid DefinitionLevelsByteLength %d", ph.DataPageHeaderV2.DefinitionLevelsByteLength)
	}
	dp.encoding = ph.DataPageHeaderV2.Encoding
	dp.ph = ph

	{ // to hide the govet shadow error
		var err error
		if dp.valuesDecoder, err = dp.fn(dp.encoding); err != nil {
			return err
		}
	}

	dataPageBlock, err := readPageBlock(r, codec, ph.GetCompressedPageSize(), ph.GetUncompressedPageSize(), validateCRC, ph.Crc)
	if err != nil {
		return err
	}

	levelsSize := ph.DataPageHeaderV2.RepetitionLevelsByteLength + ph.DataPageHeaderV2.DefinitionLevelsByteLength

	if ph.DataPageHeaderV2.RepetitionLevelsByteLength > 0 {
		if err = dp.rDecoder.init(bytes.NewReader(dataPageBlock[:int(ph.DataPageHeaderV2.RepetitionLevelsByteLength)])); err != nil {
			return fmt.Errorf("read repetition level failed: %w", err)
		}
	}

	if ph.DataPageHeaderV2.DefinitionLevelsByteLength > 0 {
		if err = dp.dDecoder.init(bytes.NewReader(dataPageBlock[int(ph.DataPageHeaderV2.RepetitionLevelsByteLength):levelsSize])); err != nil {
			return fmt.Errorf("read definition level failed: %w", err)
		}
	}

	reader, err := newBlockReader(dataPageBlock[levelsSize:], codec, ph.GetCompressedPageSize()-levelsSize, ph.GetUncompressedPageSize()-levelsSize)
	if err != nil {
		return err
	}

	return dp.valuesDecoder.init(reader)
}

type dataPageWriterV2 struct {
	dictValues []any
	col        *Column
	codec      parquet.CompressionCodec
	page       *dataPage

	dictionary bool
	enableCRC  bool
}

func (dp *dataPageWriterV2) init(col *Column, codec parquet.CompressionCodec) error {
	dp.col = col
	dp.codec = codec
	return nil
}

func (dp *dataPageWriterV2) getHeader(comp, unComp, defSize, repSize int, isCompressed bool, pageStats *parquet.Statistics, numRows int32, crc32Checksum *int32) *parquet.PageHeader {
	enc := dp.col.data.encoding()
	if dp.dictionary {
		enc = parquet.Encoding_RLE_DICTIONARY
	}
	ph := &parquet.PageHeader{
		Type:                 parquet.PageType_DATA_PAGE_V2,
		UncompressedPageSize: int32(unComp + defSize + repSize),
		CompressedPageSize:   int32(comp + defSize + repSize),
		Crc:                  crc32Checksum,
		DataPageHeaderV2: &parquet.DataPageHeaderV2{
			NumValues:                  int32(dp.page.numValues) + int32(dp.page.nullValues),
			NumNulls:                   int32(dp.page.nullValues),
			NumRows:                    numRows,
			Encoding:                   enc,
			DefinitionLevelsByteLength: int32(defSize),
			RepetitionLevelsByteLength: int32(repSize),
			IsCompressed:               isCompressed,
			Statistics:                 pageStats,
		},
	}
	return ph
}

func (dp *dataPageWriterV2) write(ctx context.Context, w io.Writer) (int, int, error) {
	rep := &bytes.Buffer{}

	// Only write repetition value higher than zero
	if dp.col.MaxRepetitionLevel() > 0 {
		if err := encodeLevelsV2(rep, dp.col.MaxRepetitionLevel(), dp.page.rL); err != nil {
			return 0, 0, err
		}
	}

	def := &bytes.Buffer{}

	// Only write definition level higher than zero
	if dp.col.MaxDefinitionLevel() > 0 {
		if err := encodeLevelsV2(def, dp.col.MaxDefinitionLevel(), dp.page.dL); err != nil {
			return 0, 0, err
		}
	}

	dataBuf := &bytes.Buffer{}
	enc := dp.col.data.encoding()

	if dp.dictionary {
		enc = parquet.Encoding_RLE_DICTIONARY
	}

	encoder, err := getValuesEncoder(enc, dp.col.Element(), dp.dictValues)
	if err != nil {
		return 0, 0, err
	}

	if err = encodeValue(dataBuf, encoder, dp.page.values); err != nil {
		return 0, 0, err
	}

	comp, err := compressBlock(dataBuf.Bytes(), dp.codec)
	if err != nil {
		return 0, 0, fmt.Errorf("compressing data failed with %s method: %w", dp.codec, err)
	}

	var crc32Checksum *int32
	if dp.enableCRC {
		v := int32(crc32.ChecksumIEEE(append(append(rep.Bytes(), def.Bytes()...), comp...)))
		crc32Checksum = &v
	}

	compSize, unCompSize := len(comp), len(dataBuf.Bytes())
	defLen, repLen := def.Len(), rep.Len()
	header := dp.getHeader(compSize, unCompSize, defLen, repLen, dp.codec != parquet.CompressionCodec_UNCOMPRESSED, dp.page.stats, int32(dp.page.numRows), crc32Checksum)
	if err := writeThrift(ctx, header, w); err != nil {
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

func newDataPageV2Writer(useDict bool, dictValues []any, page *dataPage, enableCRC bool) pageWriter {
	return &dataPageWriterV2{
		dictionary: useDict,
		dictValues: dictValues,
		page:       page,
		enableCRC:  enableCRC,
	}
}
