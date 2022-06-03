package goparquet

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"hash/crc32"
	"io"

	"github.com/fraugster/parquet-go/parquet"
)

// dictionaryPage is not a real data page, so there is no need to implement the page interface
type dictPageReader struct {
	values []interface{}
	enc    valuesDecoder
	ph     *parquet.PageHeader

	numValues   int32
	validateCRC bool

	alloc *allocTracker
}

func (dp *dictPageReader) init(dict valuesDecoder) error {
	if dict == nil {
		return errors.New("dictionary page without dictionary value encoder")
	}

	dp.enc = dict
	return nil
}

func (dp *dictPageReader) read(r io.Reader, ph *parquet.PageHeader, codec parquet.CompressionCodec) error {
	if ph.DictionaryPageHeader == nil {
		return fmt.Errorf("null DictionaryPageHeader in %+v", ph)
	}

	if dp.numValues = ph.DictionaryPageHeader.NumValues; dp.numValues < 0 {
		return fmt.Errorf("negative NumValues in DICTIONARY_PAGE: %d", dp.numValues)
	}

	if ph.DictionaryPageHeader.Encoding != parquet.Encoding_PLAIN && ph.DictionaryPageHeader.Encoding != parquet.Encoding_PLAIN_DICTIONARY {
		return fmt.Errorf("only Encoding_PLAIN and Encoding_PLAIN_DICTIONARY is supported for dict values encoder")
	}

	dp.ph = ph

	dictPageBlock, err := readPageBlock(r, codec, ph.GetCompressedPageSize(), ph.GetUncompressedPageSize(), dp.validateCRC, ph.Crc, dp.alloc)
	if err != nil {
		return err
	}

	reader, err := newBlockReader(dictPageBlock, codec, ph.GetCompressedPageSize(), ph.GetUncompressedPageSize(), dp.alloc)
	if err != nil {
		return err
	}

	dp.values = make([]interface{}, dp.numValues)

	if err := dp.enc.init(reader); err != nil {
		return err
	}

	// no error is accepted here, even EOF
	if n, err := dp.enc.decodeValues(dp.values); err != nil {
		return fmt.Errorf("expected %d values, read %d values: %w", dp.numValues, n, err)
	}

	return nil
}

type dictPageWriter struct {
	sch        *schema
	col        *Column
	codec      parquet.CompressionCodec
	dictValues []interface{}
}

func (dp *dictPageWriter) init(sch *schema, col *Column, codec parquet.CompressionCodec, dictValues []interface{}) error {
	dp.sch = sch
	dp.col = col
	dp.codec = codec
	dp.dictValues = dictValues
	return nil
}

func (dp *dictPageWriter) getHeader(comp, unComp int, crc32Checksum *int32) *parquet.PageHeader {
	ph := &parquet.PageHeader{
		Type:                 parquet.PageType_DICTIONARY_PAGE,
		UncompressedPageSize: int32(unComp),
		CompressedPageSize:   int32(comp),
		Crc:                  crc32Checksum,
		DictionaryPageHeader: &parquet.DictionaryPageHeader{
			NumValues: int32(len(dp.dictValues)),
			Encoding:  parquet.Encoding_PLAIN, // PLAIN_DICTIONARY is deprecated in the Parquet 2.0 specification
			IsSorted:  nil,
		},
	}
	return ph
}

func (dp *dictPageWriter) write(ctx context.Context, w io.Writer) (int, int, error) {
	// In V1 data page is compressed separately
	dataBuf := &bytes.Buffer{}

	encoder, err := getDictValuesEncoder(dp.col.Element())
	if err != nil {
		return 0, 0, err
	}

	err = encodeValue(dataBuf, encoder, dp.dictValues)
	if err != nil {
		return 0, 0, err
	}

	comp, err := compressBlock(dataBuf.Bytes(), dp.codec)
	if err != nil {
		return 0, 0, fmt.Errorf("compressing data failed with %s method: %w", dp.codec, err)
	}
	compSize, unCompSize := len(comp), len(dataBuf.Bytes())

	var crc32Checksum *int32
	if dp.sch.enableCRC {
		sum := int32(crc32.ChecksumIEEE(comp))
		crc32Checksum = &sum
	}

	header := dp.getHeader(compSize, unCompSize, crc32Checksum)
	if err := writeThrift(ctx, header, w); err != nil {
		return 0, 0, err
	}

	return compSize, unCompSize, writeFull(w, comp)
}
