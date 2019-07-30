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

		// Should convert to string? enums are not supported in go, so they are simply string
		// TODO : ENUM decoder/encoder
		if *typ.ConvertedType == parquet.ConvertedType_UTF8 || *typ.ConvertedType == parquet.ConvertedType_ENUM {
			return &stringEncoder{bytesArrayEncoder: ret}, nil
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

		// Should convert to string? enums are not supported in go, so they are simply string
		if *typ.ConvertedType == parquet.ConvertedType_UTF8 || *typ.ConvertedType == parquet.ConvertedType_ENUM {
			return &stringEncoder{bytesArrayEncoder: ret}, nil
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
		if *typ.ConvertedType == parquet.ConvertedType_UINT_8 || *typ.ConvertedType == parquet.ConvertedType_UINT_16 || *typ.ConvertedType == parquet.ConvertedType_UINT_32 {
			unSigned = true
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
		if *typ.ConvertedType == parquet.ConvertedType_UINT_64 {
			unSigned = true
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

	compression parquet.CompressionCodec
}

func (dp *dataPageWriterV1) init(schema *Schema, path string) error {
	col, err := schema.findDataColumn(path)
	if err != nil {
		return err
	}

	dp.col = col
	return nil
}

func (dp *dataPageWriterV1) getHeader(comp, uncomp int) *parquet.PageHeader {
	ph := &parquet.PageHeader{
		Type:                 parquet.PageType_DATA_PAGE,
		UncompressedPageSize: int32(uncomp),
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

func (dp *dataPageWriterV1) Write(w io.Writer) error {
	// In V1 data page is compressed separately
	levelBuf := &bytes.Buffer{}
	nested := strings.IndexByte(dp.col.FlatName(), '.') >= 0
	// if it is nested or it is not repeated we need the dLevel data
	if nested || dp.col.data.repetitionType() != parquet.FieldRepetitionType_REQUIRED {
		if err := encodeLevels(levelBuf, dp.col.MaxDefinitionLevel(), dp.col.data.definitionLevels()); err != nil {
			return err
		}
	}
	// if this is nested or if the data is repeated
	if nested || dp.col.data.repetitionType() == parquet.FieldRepetitionType_REPEATED {
		if err := encodeLevels(levelBuf, dp.col.MaxRepetitionLevel(), dp.col.data.repetitionLevels()); err != nil {
			return err
		}
	}

	dataBuf := &bytes.Buffer{}
	encoder, err := getValuesEncoder(dp.col.data.encoding(), dp.col.Element(), dp.col.data.dictionary())
	if err != nil {
		return err
	}

	if err := encodeValue(dataBuf, encoder, dp.col.data.dictionary().assemble(false)); err != nil {
		return err
	}

	comp, err := compressBlock(dataBuf.Bytes(), dp.compression)
	if err != nil {
		return errors.Wrapf(err, "compressing data failed with %s method", dp.compression)
	}

	header := dp.getHeader(len(comp), len(dataBuf.Bytes()))
	if err := writeThrift(header, w); err != nil {
		return err
	}

	if err := writeFull(w, levelBuf.Bytes()); err != nil {
		return err
	}

	return writeFull(w, comp)
}
