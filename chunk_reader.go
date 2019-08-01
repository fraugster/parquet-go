package go_parquet

import (
	"fmt"
	"io"
	"math/bits"
	"strings"

	"github.com/pkg/errors"

	"github.com/fraugster/parquet-go/parquet"
)

var (
	errEndOfChunk = errors.New("End Of Chunk")
)

// columnChunkReader allows to read data from a single column chunk of a parquet
// file.
// TODO: get rid of this type, Reader should return an Page
type columnChunkReader struct {
	col Column

	reader    *offsetReader
	meta      *parquet.FileMetaData
	chunkMeta *parquet.ColumnMetaData

	// Definition and repetition decoder
	rDecoder, dDecoder getLevelDecoder

	dictPage *dictPageReader

	activePage pageReader
}

type getValueDecoderFn func(parquet.Encoding) (valuesDecoder, error)
type getLevelDecoder func(parquet.Encoding) (levelDecoder, error)

func getDictValuesDecoder(typ *parquet.SchemaElement) (valuesDecoder, error) {
	switch *typ.Type {
	case parquet.Type_BYTE_ARRAY:
		ret := &byteArrayPlainDecoder{}
		if typ.ConvertedType != nil {
			if *typ.ConvertedType == parquet.ConvertedType_UTF8 || *typ.ConvertedType == parquet.ConvertedType_ENUM {
				return &stringDecoder{bytesArrayDecoder: ret}, nil
			}
		}

		if typ.LogicalType != nil && (typ.LogicalType.STRING != nil || typ.LogicalType.ENUM != nil) {
			return &stringDecoder{bytesArrayDecoder: ret}, nil
		}
		return ret, nil
	case parquet.Type_FIXED_LEN_BYTE_ARRAY:
		if typ.TypeLength == nil {
			return nil, errors.Errorf("type %s with nil type len", typ)
		}
		ret := &byteArrayPlainDecoder{length: int(*typ.TypeLength)}
		if typ.ConvertedType != nil {
			if *typ.ConvertedType == parquet.ConvertedType_UTF8 || *typ.ConvertedType == parquet.ConvertedType_ENUM {
				return &stringDecoder{bytesArrayDecoder: ret}, nil
			}
		}

		if typ.LogicalType != nil && (typ.LogicalType.STRING != nil || typ.LogicalType.ENUM != nil) {
			return &stringDecoder{bytesArrayDecoder: ret}, nil
		}
		return ret, nil
	case parquet.Type_FLOAT:
		return &floatPlainDecoder{}, nil
	case parquet.Type_DOUBLE:
		return &doublePlainDecoder{}, nil
	case parquet.Type_INT32:
		var unSigned bool
		if typ.ConvertedType != nil {
			if *typ.ConvertedType == parquet.ConvertedType_UINT_8 || *typ.ConvertedType == parquet.ConvertedType_UINT_16 || *typ.ConvertedType == parquet.ConvertedType_UINT_32 {
				unSigned = true
			}
		}
		if typ.LogicalType != nil && typ.LogicalType.INTEGER != nil && !typ.LogicalType.INTEGER.IsSigned {
			unSigned = true
		}
		return &int32PlainDecoder{unSigned: unSigned}, nil
	case parquet.Type_INT64:
		var unSigned bool
		if typ.ConvertedType != nil {
			if *typ.ConvertedType == parquet.ConvertedType_UINT_64 {
				unSigned = true
			}
		}
		if typ.LogicalType != nil && typ.LogicalType.INTEGER != nil && !typ.LogicalType.INTEGER.IsSigned {
			unSigned = true
		}
		return &int64PlainDecoder{unSigned: unSigned}, nil
	case parquet.Type_INT96:
		return &int96PlainDecoder{}, nil
	}

	return nil, errors.Errorf("type %s is not supported for dict value encoder", typ)
}

func getValuesDecoder(pageEncoding parquet.Encoding, typ *parquet.SchemaElement, dictValues []interface{}) (valuesDecoder, error) {
	// Change the deprecated value
	if pageEncoding == parquet.Encoding_PLAIN_DICTIONARY {
		pageEncoding = parquet.Encoding_RLE_DICTIONARY
	}

	switch *typ.Type {
	case parquet.Type_BOOLEAN:
		switch pageEncoding {
		case parquet.Encoding_PLAIN:
			return &booleanPlainDecoder{}, nil
		case parquet.Encoding_RLE:
			return &booleanRLEDecoder{}, nil
		case parquet.Encoding_RLE_DICTIONARY:
			return &dictDecoder{values: dictValues}, nil
		}

	case parquet.Type_BYTE_ARRAY:
		var ret bytesArrayDecoder
		switch pageEncoding {
		case parquet.Encoding_PLAIN:
			ret = &byteArrayPlainDecoder{}
		case parquet.Encoding_DELTA_LENGTH_BYTE_ARRAY:
			ret = &byteArrayDeltaLengthDecoder{}
		case parquet.Encoding_DELTA_BYTE_ARRAY:
			ret = &byteArrayDeltaDecoder{}
		case parquet.Encoding_RLE_DICTIONARY:
			ret = &dictDecoder{values: dictValues}
		}

		if ret == nil {
			break
		}
		if typ.ConvertedType != nil {
			// Should convert to string? enums are not supported in go, so they are simply string
			if *typ.ConvertedType == parquet.ConvertedType_UTF8 || *typ.ConvertedType == parquet.ConvertedType_ENUM {
				return &stringDecoder{bytesArrayDecoder: ret}, nil
			}
		}
		if typ.LogicalType != nil && (typ.LogicalType.STRING != nil || typ.LogicalType.ENUM != nil) {
			return &stringDecoder{bytesArrayDecoder: ret}, nil
		}

		return ret, nil

	case parquet.Type_FIXED_LEN_BYTE_ARRAY:
		var ret bytesArrayDecoder
		switch pageEncoding {
		case parquet.Encoding_PLAIN:
			// Not sure if this is the only case
			if typ.LogicalType != nil && typ.LogicalType.UUID != nil {
				return &uuidDecoder{}, nil
			}

			if typ.TypeLength == nil {
				return nil, errors.Errorf("type %s with nil type len", typ.Type)
			}

			ret = &byteArrayPlainDecoder{length: int(*typ.TypeLength)}
		case parquet.Encoding_DELTA_BYTE_ARRAY:
			ret = &byteArrayDeltaDecoder{}
		case parquet.Encoding_RLE_DICTIONARY:
			ret = &dictDecoder{values: dictValues}
		}
		if ret == nil {
			break
		}

		// Should convert to string? enums are not supported in go, so they are simply string
		if typ.ConvertedType != nil {
			if *typ.ConvertedType == parquet.ConvertedType_UTF8 || *typ.ConvertedType == parquet.ConvertedType_ENUM {
				return &stringDecoder{bytesArrayDecoder: ret}, nil
			}
		}
		if typ.LogicalType != nil && (typ.LogicalType.STRING != nil || typ.LogicalType.ENUM != nil) {
			return &stringDecoder{bytesArrayDecoder: ret}, nil
		}

		return ret, nil
	case parquet.Type_FLOAT:
		switch pageEncoding {
		case parquet.Encoding_PLAIN:
			return &floatPlainDecoder{}, nil
		case parquet.Encoding_RLE_DICTIONARY:
			return &dictDecoder{values: dictValues}, nil
		}

	case parquet.Type_DOUBLE:
		switch pageEncoding {
		case parquet.Encoding_PLAIN:
			return &doublePlainDecoder{}, nil
		case parquet.Encoding_RLE_DICTIONARY:
			return &dictDecoder{values: dictValues}, nil
		}

	case parquet.Type_INT32:
		var unSigned bool
		if typ.ConvertedType != nil {
			if *typ.ConvertedType == parquet.ConvertedType_UINT_8 || *typ.ConvertedType == parquet.ConvertedType_UINT_16 || *typ.ConvertedType == parquet.ConvertedType_UINT_32 {
				unSigned = true
			}
		}
		if typ.LogicalType != nil && typ.LogicalType.INTEGER != nil && !typ.LogicalType.INTEGER.IsSigned {
			unSigned = true
		}
		switch pageEncoding {
		case parquet.Encoding_PLAIN:
			return &int32PlainDecoder{unSigned: unSigned}, nil
		case parquet.Encoding_DELTA_BINARY_PACKED:
			return &int32DeltaBPDecoder{unSigned: unSigned}, nil
		case parquet.Encoding_RLE_DICTIONARY:
			return &dictDecoder{values: dictValues}, nil
		}

	case parquet.Type_INT64:
		var unSigned bool
		if typ.ConvertedType != nil {
			if *typ.ConvertedType == parquet.ConvertedType_UINT_64 {
				unSigned = true
			}
		}
		if typ.LogicalType != nil && typ.LogicalType.INTEGER != nil && !typ.LogicalType.INTEGER.IsSigned {
			unSigned = true
		}
		switch pageEncoding {
		case parquet.Encoding_PLAIN:
			return &int64PlainDecoder{unSigned: unSigned}, nil
		case parquet.Encoding_DELTA_BINARY_PACKED:
			return &int64DeltaBPDecoder{unSigned: unSigned}, nil
		case parquet.Encoding_RLE_DICTIONARY:
			return &dictDecoder{values: dictValues}, nil
		}

	case parquet.Type_INT96:
		switch pageEncoding {
		case parquet.Encoding_PLAIN:
			return &int96PlainDecoder{}, nil
		case parquet.Encoding_RLE_DICTIONARY:
			return &dictDecoder{values: dictValues}, nil
		}

	default:
		return nil, errors.Errorf("unsupported type: %s", typ.Type)
	}

	return nil, errors.Errorf("unsupported encoding %s for %s type", pageEncoding, typ.Type)
}

func newColumnChunkReader(r io.ReadSeeker, meta *parquet.FileMetaData, col Column, chunk *parquet.ColumnChunk) (*columnChunkReader, error) {
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
	// Seek to the beginning of the first Page
	_, err := r.Seek(offset, io.SeekStart)
	if err != nil {
		return nil, err
	}

	cr := &columnChunkReader{
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
		cr.dDecoder = func(parquet.Encoding) (levelDecoder, error) {
			return &levelDecoderWrapper{decoder: constDecoder(int32(col.MaxDefinitionLevel())), max: cr.col.MaxDefinitionLevel()}, nil
		}
	} else {
		cr.dDecoder = func(enc parquet.Encoding) (levelDecoder, error) {
			if enc != parquet.Encoding_RLE {
				return nil, errors.Errorf("%q is not supported for definition and repetition level", enc)
			}
			dec := newHybridDecoder(bits.Len16(col.MaxDefinitionLevel()))
			dec.buffered = true
			return &levelDecoderWrapper{decoder: dec, max: cr.col.MaxDefinitionLevel()}, nil
		}
	}
	if !nested && repType != parquet.FieldRepetitionType_REPEATED {
		// TODO: I think we need to check all schemaElements in the path
		// TODO: clarify the following comment from parquet-format/README:
		// If the column is not nested the repetition levels are not encoded and
		// always have the value of 1
		cr.rDecoder = func(parquet.Encoding) (levelDecoder, error) {
			return &levelDecoderWrapper{decoder: constDecoder(0), max: cr.col.MaxRepetitionLevel()}, nil
		}
	} else {
		cr.rDecoder = func(enc parquet.Encoding) (levelDecoder, error) {
			if enc != parquet.Encoding_RLE {
				return nil, errors.Errorf("%q is not supported for definition and repetition level", enc)
			}
			dec := newHybridDecoder(bits.Len16(col.MaxRepetitionLevel()))
			dec.buffered = true
			return &levelDecoderWrapper{decoder: dec, max: cr.col.MaxRepetitionLevel()}, nil
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

func (cr *columnChunkReader) readPage() (pageReader, error) {
	if cr.chunkMeta.TotalCompressedSize-cr.reader.Count() <= 0 {
		return nil, errEndOfChunk
	}
	ph := &parquet.PageHeader{}
	if err := readThrift(ph, cr.reader); err != nil {
		return nil, err
	}

	if ph.Type == parquet.PageType_DICTIONARY_PAGE {
		if cr.dictPage != nil {
			return nil, errors.New("there should be only one dictionary")
		}
		p := &dictPageReader{}
		de, err := getDictValuesDecoder(cr.col.Element())
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
		// Go to the next data Page
		// if we have a DictionaryPageOffset we should return to DataPageOffset
		if cr.chunkMeta.DictionaryPageOffset != nil {
			if *cr.chunkMeta.DictionaryPageOffset != cr.reader.offset {
				if _, err := cr.reader.Seek(cr.chunkMeta.DataPageOffset, io.SeekStart); err != nil {
					return nil, err
				}
			}
		}

		// Return the real data Page, not the dictionary Page
		return cr.readPage()
	}

	var p pageReader
	switch ph.Type {
	case parquet.PageType_DATA_PAGE:
		p = &dataPageReaderV1{
			ph: ph,
		}
	case parquet.PageType_DATA_PAGE_V2:
		p = &dataPageReaderV2{
			ph: ph,
		}
	default:
		return nil, errors.Errorf("DATA_PAGE or DATA_PAGE_V2 type expected, but was %s", ph.Type)
	}
	var dictValue []interface{}
	if cr.dictPage != nil {
		dictValue = cr.dictPage.values
	}
	var fn = func(typ parquet.Encoding) (valuesDecoder, error) {
		return getValuesDecoder(typ, cr.col.Element(), dictValue)
	}
	if err := p.init(cr.dDecoder, cr.rDecoder, fn); err != nil {
		return nil, err
	}

	if err := p.read(cr.reader, ph, cr.chunkMeta.Codec); err != nil {
		return nil, err
	}

	return p, nil
}

func (cr *columnChunkReader) Read(values []interface{}) (n int, dLevel []uint16, rLevel []uint16, err error) {
	// TODO : For pageV1 there is only one page per chunk, but for V2 it can be more then one. we need to re-write this
	if cr.activePage == nil {
		cr.activePage, err = cr.readPage()
		if err == errEndOfChunk { // if this is the end of chunk
			return n, dLevel, rLevel, nil
		}

		if err != nil {
			return 0, nil, nil, errors.Wrap(err, "read page failed")
		}
	}

	return cr.activePage.readValues(values[n:])
}
