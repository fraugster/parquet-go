package go_parquet

import (
	"strings"

	"github.com/pkg/errors"
	"github.com/fraugster/parquet-go/parquet"
)

func getValuesEncoder(pageEncoding parquet.Encoding, typ *parquet.SchemaElement, store *dictStore) (valuesEncoder, error) {
	// Change the deprecated value
	if pageEncoding == parquet.Encoding_PLAIN_DICTIONARY {
		pageEncoding = parquet.Encoding_RLE_DICTIONARY
	}

	switch *typ.Type {
	case parquet.Type_BOOLEAN:
		switch pageEncoding {
		case parquet.Encoding_PLAIN:
			return &booleanPlainEncoder{}, nil
		case parquet.Encoding_RLE:
			return &booleanRLEEncoder{}, nil
		case parquet.Encoding_RLE_DICTIONARY:
			return &dictEncoder{dictStore: *store}, nil
		}

	case parquet.Type_BYTE_ARRAY:
		var ret bytesArrayEncoder
		switch pageEncoding {
		case parquet.Encoding_PLAIN:
			ret = &byteArrayPlainEncoder{}
		case parquet.Encoding_DELTA_LENGTH_BYTE_ARRAY:
			ret = &byteArrayDeltaLengthEncoder{}
		case parquet.Encoding_DELTA_BYTE_ARRAY:
			ret = &byteArrayDeltaEncoder{}
		case parquet.Encoding_RLE_DICTIONARY:
			ret = &dictEncoder{dictStore: *store}
		}

		if ret == nil {
			break
		}

		if typ.ConvertedType != nil {
			// Should convert to string? enums are not supported in go, so they are simply string
			// TODO : ENUM decoder/encoder
			if *typ.ConvertedType == parquet.ConvertedType_UTF8 || *typ.ConvertedType == parquet.ConvertedType_ENUM {
				return &stringEncoder{bytesArrayEncoder: ret}, nil
			}
		}

		if typ.LogicalType != nil && (typ.LogicalType.STRING != nil || typ.LogicalType.ENUM != nil) {
			return &stringEncoder{bytesArrayEncoder: ret}, nil
		}

		return ret, nil

	case parquet.Type_FIXED_LEN_BYTE_ARRAY:
		var ret bytesArrayEncoder
		switch pageEncoding {
		case parquet.Encoding_PLAIN:
			// Not sure if this is the only case
			if typ.LogicalType != nil && typ.LogicalType.UUID != nil {
				return &uuidEncoder{}, nil
			}

			if typ.TypeLength == nil {
				return nil, errors.Errorf("type %s with nil type len", typ.Type)
			}

			ret = &byteArrayPlainEncoder{length: int(*typ.TypeLength)}
		case parquet.Encoding_DELTA_BYTE_ARRAY:
			ret = &byteArrayDeltaEncoder{}
		case parquet.Encoding_RLE_DICTIONARY:
			ret = &dictEncoder{
				dictStore: *store,
			}
		}
		if ret == nil {
			break
		}

		if typ.ConvertedType != nil {
			// Should convert to string? enums are not supported in go, so they are simply string
			if *typ.ConvertedType == parquet.ConvertedType_UTF8 || *typ.ConvertedType == parquet.ConvertedType_ENUM {
				return &stringEncoder{bytesArrayEncoder: ret}, nil
			}
		}

		if typ.LogicalType != nil && (typ.LogicalType.STRING != nil || typ.LogicalType.ENUM != nil) {
			return &stringEncoder{bytesArrayEncoder: ret}, nil
		}

		return ret, nil
	case parquet.Type_FLOAT:
		switch pageEncoding {
		case parquet.Encoding_PLAIN:
			return &floatPlainEncoder{}, nil
		case parquet.Encoding_RLE_DICTIONARY:
			return &dictEncoder{
				dictStore: *store,
			}, nil
		}

	case parquet.Type_DOUBLE:
		switch pageEncoding {
		case parquet.Encoding_PLAIN:
			return &doublePlainEncoder{}, nil
		case parquet.Encoding_RLE_DICTIONARY:
			return &dictEncoder{
				dictStore: *store,
			}, nil
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
			return &int32PlainEncoder{unSigned: unSigned}, nil
		case parquet.Encoding_DELTA_BINARY_PACKED:
			return &int32DeltaBPEncoder{unSigned: unSigned}, nil
		case parquet.Encoding_RLE_DICTIONARY:
			return &dictEncoder{
				dictStore: *store,
			}, nil
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
			return &int64PlainEncoder{unSigned: unSigned}, nil
		case parquet.Encoding_DELTA_BINARY_PACKED:
			return &int64DeltaBPEncoder{unSigned: unSigned}, nil
		case parquet.Encoding_RLE_DICTIONARY:
			return &dictEncoder{
				dictStore: *store,
			}, nil
		}

	case parquet.Type_INT96:
		switch pageEncoding {
		case parquet.Encoding_PLAIN:
			return &int96PlainEncoder{}, nil
		case parquet.Encoding_RLE_DICTIONARY:
			return &dictEncoder{
				dictStore: *store,
			}, nil
		}

	default:
		return nil, errors.Errorf("unsupported type: %s", typ.Type)
	}

	return nil, errors.Errorf("unsupported encoding %s for %s type", pageEncoding, typ.Type)
}

func getDictValuesEncoder(typ *parquet.SchemaElement) (valuesEncoder, error) {
	switch *typ.Type {
	case parquet.Type_BYTE_ARRAY:
		ret := &byteArrayPlainEncoder{}
		if typ.ConvertedType != nil {
			if *typ.ConvertedType == parquet.ConvertedType_UTF8 || *typ.ConvertedType == parquet.ConvertedType_ENUM {
				return &stringEncoder{bytesArrayEncoder: ret}, nil
			}
		}

		if typ.LogicalType != nil && (typ.LogicalType.STRING != nil || typ.LogicalType.ENUM != nil) {
			return &stringEncoder{bytesArrayEncoder: ret}, nil
		}
		return ret, nil
	case parquet.Type_FIXED_LEN_BYTE_ARRAY:
		if typ.TypeLength == nil {
			return nil, errors.Errorf("type %s with nil type len", typ)
		}
		ret := &byteArrayPlainEncoder{length: int(*typ.TypeLength)}
		if typ.ConvertedType != nil {
			if *typ.ConvertedType == parquet.ConvertedType_UTF8 || *typ.ConvertedType == parquet.ConvertedType_ENUM {
				return &stringEncoder{bytesArrayEncoder: ret}, nil
			}
		}

		if typ.LogicalType != nil && (typ.LogicalType.STRING != nil || typ.LogicalType.ENUM != nil) {
			return &stringEncoder{bytesArrayEncoder: ret}, nil
		}
		return ret, nil
	case parquet.Type_FLOAT:
		return &floatPlainEncoder{}, nil
	case parquet.Type_DOUBLE:
		return &doublePlainEncoder{}, nil
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
		return &int32PlainEncoder{unSigned: unSigned}, nil
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
		return &int64PlainEncoder{unSigned: unSigned}, nil
	case parquet.Type_INT96:
		return &int96PlainEncoder{}, nil
	}

	return nil, errors.Errorf("type %s is not supported for dict value encoder", typ)
}

func writeChunk(w writePos, schema SchemaWriter, col *column, codec parquet.CompressionCodec) (*parquet.ColumnChunk, error) {
	pos := w.Pos() // Save the position before writing data
	chunkOffset := pos
	// TODO: support more data page version? or just latest?
	var (
		dictPageOffset *int64
		useDict        bool
		// NOTE :
		// This is documentation on these two field :
		//  - TotalUncompressedSize: total byte size of all uncompressed pages in this column chunk (including the headers) *
		//  - TotalCompressedSize: total byte size of all compressed pages in this column chunk (including the headers) *
		// the including header part is confusing. for uncompressed size, we can use the position, but for the compressed
		// the only value we have doesn't contain the header
		totalComp   int64
		totalUnComp int64
	)
	if col.data.useDictionary() {
		useDict = true
		tmp := pos // make a copy, do not use the pos here
		dictPageOffset = &tmp
		dict := &dictPageWriter{}
		if err := dict.init(schema, col, codec); err != nil {
			return nil, err
		}
		compSize, unCompSize, err := dict.write(w)
		if err != nil {
			return nil, err
		}
		totalComp = w.Pos() - pos
		// Header size plus the rLevel and dLevel size
		headerSize := totalComp - int64(compSize)
		totalUnComp = int64(unCompSize) + headerSize
		pos = w.Pos() // Move position for data pos
	}

	page := &dataPageWriterV1{
		dictionary: useDict,
	}

	if err := page.init(schema, col, codec); err != nil {
		return nil, err
	}

	compSize, unCompSize, err := page.write(w)
	if err != nil {
		return nil, err
	}

	totalComp += w.Pos() - pos
	// Header size plus the rLevel and dLevel size
	headerSize := totalComp - int64(compSize)
	totalUnComp += int64(unCompSize) + headerSize

	encodings := make([]parquet.Encoding, 0, 3)
	encodings = append(encodings,
		parquet.Encoding_RLE, // TODO: used For rLevel and dLevel is it required here to?
		col.data.encoding(),
	)
	if useDict {
		encodings[1] = parquet.Encoding_PLAIN // In dictionary we use PLAIN for the data, not the column encoding
		encodings = append(encodings, parquet.Encoding_RLE_DICTIONARY)
	}

	ch := &parquet.ColumnChunk{
		FilePath:   nil, // No support for external
		FileOffset: chunkOffset,
		MetaData: &parquet.ColumnMetaData{
			Type:                  col.data.parquetType(),
			Encodings:             encodings,
			PathInSchema:          strings.Split(col.flatName, "."),
			Codec:                 codec,
			NumValues:             schema.NumRecords(),
			TotalUncompressedSize: totalUnComp,
			TotalCompressedSize:   totalComp,
			KeyValueMetadata:      nil, // TODO: add key/value metadata support
			DataPageOffset:        pos,
			IndexPageOffset:       nil,
			DictionaryPageOffset:  dictPageOffset,
			Statistics:            nil, // TODO: add statistics
			EncodingStats:         nil,
			BloomFilterOffset:     nil,
		},
		OffsetIndexOffset: nil,
		OffsetIndexLength: nil,
		ColumnIndexOffset: nil,
		ColumnIndexLength: nil,
	}

	return ch, nil
}

func writeRowGroup(w writePos, schema SchemaWriter, codec parquet.CompressionCodec) ([]*parquet.ColumnChunk, error) {
	dataCols := schema.Columns()
	var res = make([]*parquet.ColumnChunk, 0, len(dataCols))
	for _, ci := range dataCols {
		// TODO: the Column is only *column, but maybe its better not to cast and find the field directly on schema again?
		ch, err := writeChunk(w, schema, ci.(*column), codec)
		if err != nil {
			return nil, err
		}

		res = append(res, ch)
	}

	return res, nil

}
