package go_parquet

import (
	"bytes"
	"fmt"
	"io"
	"io/ioutil"
	"math/bits"
	"strings"

	"github.com/pkg/errors"

	"github.com/fraugster/parquet-go/parquet"
)

// ColumnChunkReader allows to read data from a single column chunk of a parquet
// file.
type ColumnChunkReader struct {
	col Column

	reader    *offsetReader
	meta      *parquet.FileMetaData
	chunkMeta *parquet.ColumnMetaData
	notFirst  bool

	// Definition and repetition decoder
	rDecoder, dDecoder func() decoder

	dictPage *DictionaryPage
}

type getValueDecoderFn func(parquet.Encoding) (valuesDecoder, error)

type page interface {
	init(dDecoder, rDecoder func() decoder, values getValueDecoderFn) error
	read(r io.ReadSeeker, ph *parquet.PageHeader, codec parquet.CompressionCodec) error
}

type valuesDecoder interface {
	init(io.Reader) error
	decodeValues(dst []interface{}) error
}

func getDictValuesEncoder(typ parquet.Type, typeLen *int32) (valuesDecoder, error) {
	switch typ {
	case parquet.Type_BYTE_ARRAY:
		return &byteArrayPlainDecoder{}, nil
	case parquet.Type_FIXED_LEN_BYTE_ARRAY:
		if typeLen == nil {
			return nil, errors.Errorf("type %s with nil type len", typ)
		}

		return &byteArrayPlainDecoder{length: int(*typeLen)}, nil
	case parquet.Type_FLOAT:
		return &floatPlainDecoder{}, nil
	case parquet.Type_DOUBLE:
		return &doublePlainDecoder{}, nil
	case parquet.Type_INT32:
		return &int32PlainDecoder{}, nil
	case parquet.Type_INT64:
		return &int64PlainDecoder{}, nil
	case parquet.Type_INT96:
		return &int96PlainDecoder{}, nil
	}

	return nil, errors.Errorf("type %s is not supported for dict value encoder", typ)
}

func getValuesDecoder(pageEncoding parquet.Encoding, typ parquet.Type, typeLen *int32, dict valuesDecoder) (valuesDecoder, error) {
	// Change the deprecated value
	if pageEncoding == parquet.Encoding_PLAIN_DICTIONARY {
		pageEncoding = parquet.Encoding_RLE_DICTIONARY
	}

	switch typ {
	case parquet.Type_BOOLEAN:
		switch pageEncoding {
		case parquet.Encoding_PLAIN:
			return &booleanPlainDecoder{}, nil
		case parquet.Encoding_RLE:
			return &booleanRLEDecoder{}, nil
		case parquet.Encoding_RLE_DICTIONARY:
			return dict, nil
		}

	case parquet.Type_BYTE_ARRAY:
		switch pageEncoding {
		case parquet.Encoding_PLAIN:
			return &byteArrayPlainDecoder{}, nil
		case parquet.Encoding_DELTA_LENGTH_BYTE_ARRAY:
			return &byteArrayDeltaLengthDecoder{}, nil
		case parquet.Encoding_DELTA_BYTE_ARRAY:
			return &byteArrayDeltaDecoder{}, nil
		case parquet.Encoding_RLE_DICTIONARY:
			return dict, nil
		}

	case parquet.Type_FIXED_LEN_BYTE_ARRAY:
		switch pageEncoding {
		case parquet.Encoding_PLAIN:
			if typeLen == nil {
				return nil, errors.Errorf("type %s with nil type len", typ)
			}

			return &byteArrayPlainDecoder{length: int(*typeLen)}, nil
		case parquet.Encoding_DELTA_BYTE_ARRAY:
			return &byteArrayDeltaDecoder{}, nil
		case parquet.Encoding_RLE_DICTIONARY:
			return dict, nil
		}

	case parquet.Type_FLOAT:
		switch pageEncoding {
		case parquet.Encoding_PLAIN:
			return &floatPlainDecoder{}, nil
		case parquet.Encoding_RLE_DICTIONARY:
			return dict, nil
		}

	case parquet.Type_DOUBLE:
		switch pageEncoding {
		case parquet.Encoding_PLAIN:
			return &doublePlainDecoder{}, nil
		case parquet.Encoding_RLE_DICTIONARY:
			return dict, nil
		}

	case parquet.Type_INT32:
		switch pageEncoding {
		case parquet.Encoding_PLAIN:
			return &int32PlainDecoder{}, nil
		case parquet.Encoding_DELTA_BINARY_PACKED:
			return &int32DeltaBPDecoder{}, nil
		case parquet.Encoding_RLE_DICTIONARY:
			return dict, nil
		}

	case parquet.Type_INT64:
		switch pageEncoding {
		case parquet.Encoding_PLAIN:
			return &int64PlainDecoder{}, nil
		case parquet.Encoding_DELTA_BINARY_PACKED:
			return &int64DeltaBPDecoder{}, nil
		case parquet.Encoding_RLE_DICTIONARY:
			return dict, nil
		}

	case parquet.Type_INT96:
		switch pageEncoding {
		case parquet.Encoding_PLAIN:
			return &int96PlainDecoder{}, nil
		case parquet.Encoding_RLE_DICTIONARY:
			return dict, nil
		}

	default:
		return nil, errors.Errorf("unsupported type: %s", typ)
	}

	return nil, errors.Errorf("unsupported encoding %s for %s type", pageEncoding, typ)
}

func newColumnChunkReader(r io.ReadSeeker, meta *parquet.FileMetaData, col Column, chunk *parquet.ColumnChunk) (*ColumnChunkReader, error) {
	if chunk.FilePath != nil {
		return nil, fmt.Errorf("nyi: data is in another file: '%s'", *chunk.FilePath)
	}

	c := col.Index()
	// chunk.FileOffset is useless so ChunkMetaData is required here
	// as we cannot read it from r
	// see https://issues.apache.org/jira/browse/PARQUET-291
	if chunk.MetaData == nil {
		return nil, errors.Errorf("missing meta data for column %c", c)
	}

	if typ := *col.Element().Type; chunk.MetaData.Type != typ {
		return nil, errors.Errorf("wrong type in column chunk metadata, expected %s was %s",
			typ, chunk.MetaData.Type)
	}

	offset := chunk.MetaData.DataPageOffset
	if chunk.MetaData.DictionaryPageOffset != nil {
		offset = *chunk.MetaData.DictionaryPageOffset
	}
	// Seek to the beginning of the first page
	_, err := r.Seek(offset, io.SeekStart)
	if err != nil {
		return nil, err
	}

	cr := &ColumnChunkReader{
		col: col,
		reader: &offsetReader{
			inner:  r,
			offset: offset,
			count:  0,
		},
		meta:      meta,
		chunkMeta: chunk.MetaData,
	}
	nested := strings.IndexByte(col.FlatName(), '.') >= 0
	repType := *col.Element().RepetitionType
	if !nested && repType == parquet.FieldRepetitionType_REQUIRED {
		// TODO: also check that len(Path) = maxD
		// For data that is required, the definition levels are not encoded and
		// always have the value of the max definition level.
		// TODO: document level ranges
		cr.dDecoder = func() decoder {
			return constDecoder(int32(col.MaxDefinitionLevel()))
		}
	} else {
		cr.dDecoder = func() decoder {
			return newHybridDecoder(bits.Len16(col.MaxDefinitionLevel()))
		}
	}
	if !nested && repType != parquet.FieldRepetitionType_REPEATED {
		// TODO: I think we need to check all schemaElements in the path (confirm if above)
		// TODO: clarify the following comment from parquet-format/README:
		// If the column is not nested the repetition levels are not encoded and
		// always have the value of 1
		cr.rDecoder = func() decoder {
			return constDecoder(0)
		}
	} else {
		cr.rDecoder = func() decoder {
			return newHybridDecoder(bits.Len16(col.MaxRepetitionLevel()))
		}
	}

	return cr, nil
}

func createDataReader(r io.Reader, codec parquet.CompressionCodec, compressedSize int32, uncompressedSize int32) (io.Reader, error) {
	if compressedSize < 0 || uncompressedSize < 0 {
		return nil, errors.New("invalid page data size")
	}

	return newBlockReader(r, codec, compressedSize, uncompressedSize)
}

type DictionaryPage struct {
	ph *parquet.PageHeader

	numValues int32
	enc       valuesDecoder

	values []interface{}
}

func (dp *DictionaryPage) init(dict valuesDecoder) error {
	if dict == nil {
		return errors.New("dictionary page without dictionary value encoder")
	}

	dp.enc = dict
	return nil
}

func (dp *DictionaryPage) read(r io.ReadSeeker, ph *parquet.PageHeader, codec parquet.CompressionCodec) error {
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

	reader, err := createDataReader(r, codec, ph.GetCompressedPageSize(), ph.GetCompressedPageSize())
	if err != nil {
		return err
	}

	dp.values = make([]interface{}, dp.numValues)
	if err := dp.enc.init(reader); err != nil {
		return err
	}
	if err := dp.enc.decodeValues(dp.values); err != nil {
		return err
	}

	return nil
}

type DataPageV1 struct {
	ph *parquet.PageHeader

	numValues          int32
	encoding           parquet.Encoding
	dDecoder, rDecoder decoder
	valuesDecoder      valuesDecoder
	fn                 getValueDecoderFn
}

func (dp *DataPageV1) init(dDecoder, rDecoder func() decoder, values getValueDecoderFn) error {
	dp.dDecoder = dDecoder()
	dp.rDecoder = rDecoder()
	dp.fn = values

	return nil
}

func (dp *DataPageV1) read(r io.ReadSeeker, ph *parquet.PageHeader, codec parquet.CompressionCodec) (err error) {
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

	reader, err := createDataReader(r, codec, ph.GetCompressedPageSize(), ph.GetCompressedPageSize())
	if err != nil {
		return err
	}

	// TODO : read the data page here
	// We need to consume all reader here
	b, err := ioutil.ReadAll(reader)
	if err != nil {
		return errors.Wrap(err, "read data page failed")
	}
	fmt.Println("Read the data page len :", len(b))
	return nil
}

type DataPageV2 struct {
	ph *parquet.PageHeader

	numValues          int32
	encoding           parquet.Encoding
	valuesDecoder      valuesDecoder
	dDecoder, rDecoder decoder
	fn                 getValueDecoderFn
}

func (dp *DataPageV2) init(dDecoder, rDecoder func() decoder, values getValueDecoderFn) error {
	dp.dDecoder = dDecoder()
	dp.rDecoder = rDecoder()
	dp.fn = values

	return nil
}

func (dp *DataPageV2) read(r io.ReadSeeker, ph *parquet.PageHeader, codec parquet.CompressionCodec) (err error) {
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

	// TODO: I am not sure if this is correct to subtract the level size from the compressed size here
	reader, err := createDataReader(r, codec, ph.GetCompressedPageSize()-levelsSize, ph.GetCompressedPageSize()-levelsSize)
	if err != nil {
		return err
	}

	// TODO : read the data page here
	// We need to consume all reader here
	b, err := ioutil.ReadAll(reader)
	if err != nil {
		return errors.Wrap(err, "read data page failed")
	}
	fmt.Println("Read the data page v2 len :", len(b))
	return nil
}

func (cr *ColumnChunkReader) readPage() (page, error) {
	if cr.chunkMeta.TotalCompressedSize-cr.reader.Count() <= 0 {
		return nil, errors.New("EndOfTheChunk")
	}
	ph := &parquet.PageHeader{}
	if err := readThrift(ph, cr.reader); err != nil {
		return nil, err
	}

	if !cr.notFirst && ph.Type == parquet.PageType_DICTIONARY_PAGE {
		cr.notFirst = true
		p := &DictionaryPage{}
		de, err := getDictValuesEncoder(*cr.col.Element().Type, cr.col.Element().TypeLength)
		if err != nil {
			return nil, err
		}
		if err := p.init(de); err != nil {
			return nil, err
		}

		if err := p.read(cr.reader, ph, cr.chunkMeta.Codec); err != nil {
			return nil, err
		}

		cr.dictPage = p
		// Go to the next data page
		// if we have a DictionaryPageOffset we should return to DataPageOffset
		if cr.chunkMeta.DictionaryPageOffset != nil {
			if *cr.chunkMeta.DictionaryPageOffset != cr.reader.offset {
				if _, err := cr.reader.Seek(cr.chunkMeta.DataPageOffset, io.SeekStart); err != nil {
					return nil, err
				}
			}
		}

		// Return the real data page, not the dictionary page
		return cr.readPage()
	}

	var p page
	switch ph.Type {
	case parquet.PageType_DATA_PAGE:
		p = &DataPageV1{}
	case parquet.PageType_DATA_PAGE_V2:
		p = &DataPageV2{}
	default:
		return nil, errors.Errorf("DATA_PAGE or DATA_PAGE_V2 type expected, but was %s", ph.Type)
	}
	var dict valuesDecoder
	if cr.dictPage != nil {
		dict = cr.dictPage.enc
	}
	var fn = func(typ parquet.Encoding) (valuesDecoder, error) {
		return getValuesDecoder(typ, *cr.col.Element().Type, cr.col.Element().TypeLength, dict)
	}
	if err := p.init(cr.dDecoder, cr.rDecoder, fn); err != nil {
		return nil, err
	}

	if err := p.read(cr.reader, ph, cr.chunkMeta.Codec); err != nil {
		return nil, err
	}

	return p, nil
}

func (cr *ColumnChunkReader) Read() (page, error) {
	return cr.readPage()
}
