package go_parquet

import (
	"bytes"
	"io"
	"strings"

	"github.com/pkg/errors"

	"github.com/fraugster/parquet-go/parquet"
)

func getValuesEncoder(pageEncoding parquet.Encoding, typ *parquet.SchemaElement, store *dictStore) (valuesEncoder, error) {
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

type dataPageWriterV1 struct {
	col *column

	codec parquet.CompressionCodec
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
	levelBuf := &bytes.Buffer{}
	nested := strings.IndexByte(dp.col.FlatName(), '.') >= 0
	// if it is nested or it is not repeated we need the dLevel data
	if nested || dp.col.data.repetitionType() != parquet.FieldRepetitionType_REQUIRED {
		if err := encodeLevels(levelBuf, dp.col.MaxDefinitionLevel(), dp.col.data.definitionLevels()); err != nil {
			return 0, 0, err
		}
	}
	// if this is nested or if the data is repeated
	if nested || dp.col.data.repetitionType() == parquet.FieldRepetitionType_REPEATED {
		if err := encodeLevels(levelBuf, dp.col.MaxRepetitionLevel(), dp.col.data.repetitionLevels()); err != nil {
			return 0, 0, err
		}
	}

	dataBuf := &bytes.Buffer{}
	encoder, err := getValuesEncoder(dp.col.data.encoding(), dp.col.Element(), dp.col.data.dictionary())
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

	if err := writeFull(w, levelBuf.Bytes()); err != nil {
		return 0, 0, err
	}

	return compSize, unCompSize, writeFull(w, comp)
}

func writeChunk(w writePos, schema *Schema, col *column, codec parquet.CompressionCodec) (*parquet.ColumnChunk, error) {
	pos := w.Pos() // Save the position before writing data
	// TODO: support more data page version? or just latest?
	page := &dataPageWriterV1{}

	if err := page.init(schema, col, codec); err != nil {
		return nil, err
	}

	compSize, unCompSize, err := page.Write(w)
	if err != nil {
		return nil, err
	}

	// TODO: these two are reminder for supporting multi-page writer
	// NOTE :
	// This is documentation on these two field :
	//  - TotalUncompressedSize: total byte size of all uncompressed pages in this column chunk (including the headers) *
	//  - TotalCompressedSize: total byte size of all compressed pages in this column chunk (including the headers) *
	// the including header part is confusing. for uncompressed size, we can use the position, but for the compressed
	// the only value we have doesn't contain the header
	totalComp := w.Pos() - pos
	// Header size plus the rLevel and dLevel size
	headerSize := totalComp - int64(compSize)
	totalUnComp := int64(unCompSize) + headerSize

	ch := &parquet.ColumnChunk{
		FilePath:   nil, // No support for external
		FileOffset: pos,
		MetaData: &parquet.ColumnMetaData{
			Type: col.data.parquetType(),
			Encodings: []parquet.Encoding{
				parquet.Encoding_RLE, // TODO: used For rLevel and dLevel is it required here to?
				col.data.encoding(),
			},
			PathInSchema:          strings.Split(col.flatName, "."),
			Codec:                 codec,
			NumValues:             schema.numRecords,
			TotalUncompressedSize: totalUnComp,
			TotalCompressedSize:   totalComp,
			KeyValueMetadata:      nil, // TODO: add key/value metadata support
			DataPageOffset:        pos,
			IndexPageOffset:       nil,
			DictionaryPageOffset:  nil,
			Statistics:            nil,
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

func writeRowGroup(w writePos, schema *Schema, codec parquet.CompressionCodec) ([]*parquet.ColumnChunk, error) {
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
