package goparquet

import (
	"fmt"
	"io"
	"math/bits"

	"github.com/pkg/errors"

	"github.com/fraugster/parquet-go/parquet"
)

type getValueDecoderFn func(parquet.Encoding) (valuesDecoder, error)
type getLevelDecoder func(parquet.Encoding) (levelDecoder, error)

func getDictValuesDecoder(typ *parquet.SchemaElement) (valuesDecoder, error) {
	switch *typ.Type {
	case parquet.Type_BYTE_ARRAY:
		return &byteArrayPlainDecoder{}, nil
	case parquet.Type_FIXED_LEN_BYTE_ARRAY:
		if typ.TypeLength == nil {
			return nil, errors.Errorf("type %s with nil type len", typ)
		}
		return &byteArrayPlainDecoder{length: int(*typ.TypeLength)}, nil
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

func getBooleanValuesDecoder(pageEncoding parquet.Encoding, dictValues []interface{}) (valuesDecoder, error) {
	switch pageEncoding {
	case parquet.Encoding_PLAIN:
		return &booleanPlainDecoder{}, nil
	case parquet.Encoding_RLE:
		return &booleanRLEDecoder{}, nil
	case parquet.Encoding_RLE_DICTIONARY:
		return &dictDecoder{values: dictValues}, nil
	default:
		return nil, errors.Errorf("unsupported encoding %s for boolean", pageEncoding)
	}
}

func getByteArrayValuesDecoder(pageEncoding parquet.Encoding, dictValues []interface{}) (valuesDecoder, error) {
	switch pageEncoding {
	case parquet.Encoding_PLAIN:
		return &byteArrayPlainDecoder{}, nil
	case parquet.Encoding_DELTA_LENGTH_BYTE_ARRAY:
		return &byteArrayDeltaLengthDecoder{}, nil
	case parquet.Encoding_DELTA_BYTE_ARRAY:
		return &byteArrayDeltaDecoder{}, nil
	case parquet.Encoding_RLE_DICTIONARY:
		return &dictDecoder{values: dictValues}, nil
	default:
		return nil, errors.Errorf("unsupported encoding %s for binary", pageEncoding)
	}
}

func getFixedLenByteArrayValuesDecoder(pageEncoding parquet.Encoding, len int, dictValues []interface{}) (valuesDecoder, error) {
	switch pageEncoding {
	case parquet.Encoding_PLAIN:
		return &byteArrayPlainDecoder{length: len}, nil
	case parquet.Encoding_DELTA_BYTE_ARRAY:
		return &byteArrayDeltaDecoder{}, nil
	case parquet.Encoding_RLE_DICTIONARY:
		return &dictDecoder{values: dictValues}, nil
	default:
		return nil, errors.Errorf("unsupported encoding %s for fixed_len_byte_array(%d)", pageEncoding, len)
	}
}

func getInt32ValuesDecoder(pageEncoding parquet.Encoding, typ *parquet.SchemaElement, dictValues []interface{}) (valuesDecoder, error) {
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
	default:
		return nil, errors.Errorf("unsupported encoding %s for int32", pageEncoding)
	}
}

func getInt64ValuesDecoder(pageEncoding parquet.Encoding, typ *parquet.SchemaElement, dictValues []interface{}) (valuesDecoder, error) {
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
	default:
		return nil, errors.Errorf("unsupported encoding %s for int64", pageEncoding)
	}
}

func getValuesDecoder(pageEncoding parquet.Encoding, typ *parquet.SchemaElement, dictValues []interface{}) (valuesDecoder, error) {
	// Change the deprecated value
	if pageEncoding == parquet.Encoding_PLAIN_DICTIONARY {
		pageEncoding = parquet.Encoding_RLE_DICTIONARY
	}

	switch *typ.Type {
	case parquet.Type_BOOLEAN:
		return getBooleanValuesDecoder(pageEncoding, dictValues)

	case parquet.Type_BYTE_ARRAY:
		return getByteArrayValuesDecoder(pageEncoding, dictValues)

	case parquet.Type_FIXED_LEN_BYTE_ARRAY:
		if typ.TypeLength == nil {
			return nil, errors.Errorf("type %s with nil type len", typ.Type)
		}
		return getFixedLenByteArrayValuesDecoder(pageEncoding, int(*typ.TypeLength), dictValues)
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
		return getInt32ValuesDecoder(pageEncoding, typ, dictValues)

	case parquet.Type_INT64:
		return getInt64ValuesDecoder(pageEncoding, typ, dictValues)

	case parquet.Type_INT96:
		switch pageEncoding {
		case parquet.Encoding_PLAIN:
			return &int96PlainDecoder{}, nil
		case parquet.Encoding_RLE_DICTIONARY:
			return &dictDecoder{values: dictValues}, nil
		}

	default:
		return nil, errors.Errorf("unsupported type %s", typ.Type)
	}

	return nil, errors.Errorf("unsupported encoding %s for %s type", pageEncoding, typ.Type)
}

func createDataReader(r io.Reader, codec parquet.CompressionCodec, compressedSize int32, uncompressedSize int32) (io.Reader, error) {
	if compressedSize < 0 || uncompressedSize < 0 {
		return nil, errors.New("invalid page data size")
	}

	return newBlockReader(r, codec, compressedSize, uncompressedSize)
}

func readPages(r *offsetReader, col *Column, chunkMeta *parquet.ColumnMetaData, dDecoder, rDecoder getLevelDecoder) ([]pageReader, error) {
	var (
		dictPage *dictPageReader
		pages    []pageReader
	)

	for {
		if chunkMeta.TotalCompressedSize-r.Count() <= 0 {
			break
		}
		ph := &parquet.PageHeader{}
		if err := readThrift(ph, r); err != nil {
			return nil, err
		}

		if ph.Type == parquet.PageType_DICTIONARY_PAGE {
			if dictPage != nil {
				return nil, errors.New("there should be only one dictionary")
			}
			p := &dictPageReader{}
			de, err := getDictValuesDecoder(col.Element())
			if err != nil {
				return nil, err
			}
			if err := p.init(de); err != nil {
				return nil, err
			}

			// re-use the value dictionary store
			p.values = col.getColumnStore().currValues.values
			if err := p.read(r, ph, chunkMeta.Codec); err != nil {
				return nil, err
			}

			dictPage = p
			// Go to the next data Page
			// if we have a DictionaryPageOffset we should return to DataPageOffset
			if chunkMeta.DictionaryPageOffset != nil {
				if *chunkMeta.DictionaryPageOffset != r.offset {
					if _, err := r.Seek(chunkMeta.DataPageOffset, io.SeekStart); err != nil {
						return nil, err
					}
				}
			}
			continue // go to next page
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
			return nil, errors.Errorf("DATA_PAGE or DATA_PAGE_V2 type supported, but was %s", ph.Type)
		}
		var dictValue []interface{}
		if dictPage != nil {
			dictValue = dictPage.values
		}
		var fn = func(typ parquet.Encoding) (valuesDecoder, error) {
			return getValuesDecoder(typ, col.Element(), dictValue)
		}
		if err := p.init(dDecoder, rDecoder, fn); err != nil {
			return nil, err
		}

		if err := p.read(r, ph, chunkMeta.Codec); err != nil {
			return nil, err
		}
		pages = append(pages, p)
	}

	return pages, nil
}

func skipChunk(r io.Seeker, col *Column, chunk *parquet.ColumnChunk) error {
	if chunk.FilePath != nil {
		return fmt.Errorf("nyi: data is in another file: '%s'", *chunk.FilePath)
	}

	c := col.Index()
	// chunk.FileOffset is useless so ChunkMetaData is required here
	// as we cannot read it from r
	// see https://issues.apache.org/jira/browse/PARQUET-291
	if chunk.MetaData == nil {
		return errors.Errorf("missing meta data for Column %c", c)
	}

	if typ := *col.Element().Type; chunk.MetaData.Type != typ {
		return errors.Errorf("wrong type in Column chunk metadata, expected %s was %s",
			typ, chunk.MetaData.Type)
	}

	offset := chunk.MetaData.DataPageOffset
	if chunk.MetaData.DictionaryPageOffset != nil {
		offset = *chunk.MetaData.DictionaryPageOffset
	}

	offset += chunk.MetaData.TotalCompressedSize
	_, err := r.Seek(offset, io.SeekStart)
	return err
}

func readChunk(r io.ReadSeeker, col *Column, chunk *parquet.ColumnChunk) ([]pageReader, error) {
	if chunk.FilePath != nil {
		return nil, fmt.Errorf("nyi: data is in another file: '%s'", *chunk.FilePath)
	}

	c := col.Index()
	// chunk.FileOffset is useless so ChunkMetaData is required here
	// as we cannot read it from r
	// see https://issues.apache.org/jira/browse/PARQUET-291
	if chunk.MetaData == nil {
		return nil, errors.Errorf("missing meta data for Column %c", c)
	}

	if typ := *col.Element().Type; chunk.MetaData.Type != typ {
		return nil, errors.Errorf("wrong type in Column chunk metadata, expected %s was %s",
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

	reader := &offsetReader{
		inner:  r,
		offset: offset,
		count:  0,
	}

	rDecoder := func(enc parquet.Encoding) (levelDecoder, error) {
		if enc != parquet.Encoding_RLE {
			return nil, errors.Errorf("%q is not supported for definition and repetition level", enc)
		}
		dec := newHybridDecoder(bits.Len16(col.MaxRepetitionLevel()))
		dec.buffered = true
		return &levelDecoderWrapper{decoder: dec, max: col.MaxRepetitionLevel()}, nil
	}

	dDecoder := func(enc parquet.Encoding) (levelDecoder, error) {
		if enc != parquet.Encoding_RLE {
			return nil, errors.Errorf("%q is not supported for definition and repetition level", enc)
		}
		dec := newHybridDecoder(bits.Len16(col.MaxDefinitionLevel()))
		dec.buffered = true
		return &levelDecoderWrapper{decoder: dec, max: col.MaxDefinitionLevel()}, nil
	}

	if col.MaxRepetitionLevel() == 0 {
		rDecoder = func(parquet.Encoding) (levelDecoder, error) {
			return &levelDecoderWrapper{decoder: constDecoder(0), max: col.MaxRepetitionLevel()}, nil
		}
	}

	if col.MaxDefinitionLevel() == 0 {
		dDecoder = func(parquet.Encoding) (levelDecoder, error) {
			return &levelDecoderWrapper{decoder: constDecoder(0), max: col.MaxDefinitionLevel()}, nil
		}
	}
	return readPages(reader, col, chunk.MetaData, dDecoder, rDecoder)
}

func readPageData(col *Column, pages []pageReader) error {
	s := col.getColumnStore()
	for i := range pages {
		data := make([]interface{}, pages[i].numValues())
		n, dl, rl, err := pages[i].readValues(data)
		if err != nil {
			return err
		}

		if int32(n) != pages[i].numValues() {
			return errors.Errorf("expect %d value but read %d", pages[i].numValues(), n)
		}

		// using append to make sure we handle the multiple data page correctly
		s.currRLevels.appendArray(rl)
		s.currDLevels.appendArray(dl)

		s.currValues.values = append(s.currValues.values, data...)
		s.currValues.noDictMode = true
	}

	return nil
}

func readRowGroup(r io.ReadSeeker, schema SchemaReader, rowGroups *parquet.RowGroup) error {
	dataCols := schema.Columns()
	schema.resetData()
	schema.setNumRecords(rowGroups.NumRows)
	for _, c := range dataCols {
		idx := c.Index()
		if len(rowGroups.Columns) <= idx {
			return fmt.Errorf("column index %d is out of bounds", idx)
		}
		chunk := rowGroups.Columns[c.Index()]
		if !schema.isSelected(c.flatName) {
			if err := skipChunk(r, c, chunk); err != nil {
				return err
			}
			c.data.skipped = true
			continue
		}
		pages, err := readChunk(r, c, chunk)
		if err != nil {
			return err
		}
		if err := readPageData(c, pages); err != nil {
			return err
		}
	}

	return nil
}
