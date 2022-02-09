package goparquet

import (
	"bytes"
	"context"
	"io"

	"github.com/fraugster/parquet-go/parquet"
	"github.com/pkg/errors"
)

// dictionaryPage is not a real data page, so there is no need to implement the page interface
type dictPageReader struct {
	ph *parquet.PageHeader

	numValues int32
	enc       valuesDecoder

	values []interface{}
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
		return errors.Errorf("null DictionaryPageHeader in %+v", ph)
	}

	if dp.numValues = ph.DictionaryPageHeader.NumValues; dp.numValues < 0 {
		return errors.Errorf("negative NumValues in DICTIONARY_PAGE: %d", dp.numValues)
	}

	if ph.DictionaryPageHeader.Encoding != parquet.Encoding_PLAIN && ph.DictionaryPageHeader.Encoding != parquet.Encoding_PLAIN_DICTIONARY {
		return errors.Errorf("only Encoding_PLAIN and Encoding_PLAIN_DICTIONARY is supported for dict values encoder")
	}

	dp.ph = ph

	reader, err := createDataReader(r, codec, ph.GetCompressedPageSize(), ph.GetUncompressedPageSize())
	if err != nil {
		return err
	}

	dp.values = make([]interface{}, dp.numValues)

	if err := dp.enc.init(reader); err != nil {
		return err
	}

	// no error is accepted here, even EOF
	if n, err := dp.enc.decodeValues(dp.values); err != nil {
		return errors.Wrapf(err, "expected %d values, read %d values", dp.numValues, n)
	}

	return nil
}

type dictPageWriter struct {
	col        *Column
	codec      parquet.CompressionCodec
	dictValues []interface{}
}

func (dp *dictPageWriter) init(schema SchemaWriter, col *Column, codec parquet.CompressionCodec, dictValues []interface{}) error {
	dp.col = col
	dp.codec = codec
	dp.dictValues = dictValues
	return nil
}

func (dp *dictPageWriter) getHeader(comp, unComp int) *parquet.PageHeader {
	ph := &parquet.PageHeader{
		Type:                 parquet.PageType_DICTIONARY_PAGE,
		UncompressedPageSize: int32(unComp),
		CompressedPageSize:   int32(comp),
		Crc:                  nil,
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
		return 0, 0, errors.Wrapf(err, "compressing data failed with %s method", dp.codec)
	}
	compSize, unCompSize := len(comp), len(dataBuf.Bytes())

	header := dp.getHeader(compSize, unCompSize)
	if err := writeThrift(ctx, header, w); err != nil {
		return 0, 0, err
	}

	return compSize, unCompSize, writeFull(w, comp)
}
