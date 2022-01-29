package goparquet

import (
	"context"
	"sort"

	"github.com/fraugster/parquet-go/parquet"
	"github.com/pkg/errors"
)

func getBooleanValuesEncoder(pageEncoding parquet.Encoding, store *dictStore) (valuesEncoder, error) {
	switch pageEncoding {
	case parquet.Encoding_PLAIN:
		return &booleanPlainEncoder{}, nil
	case parquet.Encoding_RLE:
		return &booleanRLEEncoder{}, nil
	case parquet.Encoding_RLE_DICTIONARY:
		return &dictEncoder{dictStore: *store}, nil
	default:
		return nil, errors.Errorf("unsupported encoding %s for boolean", pageEncoding)
	}
}

func getByteArrayValuesEncoder(pageEncoding parquet.Encoding, store *dictStore) (valuesEncoder, error) {
	switch pageEncoding {
	case parquet.Encoding_PLAIN:
		return &byteArrayPlainEncoder{}, nil
	case parquet.Encoding_DELTA_LENGTH_BYTE_ARRAY:
		return &byteArrayDeltaLengthEncoder{}, nil
	case parquet.Encoding_DELTA_BYTE_ARRAY:
		return &byteArrayDeltaEncoder{}, nil
	case parquet.Encoding_RLE_DICTIONARY:
		return &dictEncoder{dictStore: *store}, nil
	default:
		return nil, errors.Errorf("unsupported encoding %s for binary", pageEncoding)
	}
}

func getFixedLenByteArrayValuesEncoder(pageEncoding parquet.Encoding, len int, store *dictStore) (valuesEncoder, error) {
	switch pageEncoding {
	case parquet.Encoding_PLAIN:
		return &byteArrayPlainEncoder{length: len}, nil
	case parquet.Encoding_DELTA_BYTE_ARRAY:
		return &byteArrayDeltaEncoder{}, nil
	case parquet.Encoding_RLE_DICTIONARY:
		return &dictEncoder{dictStore: *store}, nil
	default:
		return nil, errors.Errorf("unsupported encoding %s for fixed_len_byte_array(%d)", pageEncoding, len)
	}
}

func getInt32ValuesEncoder(pageEncoding parquet.Encoding, typ *parquet.SchemaElement, store *dictStore) (valuesEncoder, error) {
	switch pageEncoding {
	case parquet.Encoding_PLAIN:
		return &int32PlainEncoder{}, nil
	case parquet.Encoding_DELTA_BINARY_PACKED:
		return &int32DeltaBPEncoder{}, nil
	case parquet.Encoding_RLE_DICTIONARY:
		return &dictEncoder{
			dictStore: *store,
		}, nil
	default:
		return nil, errors.Errorf("unsupported encoding %s for int32", pageEncoding)
	}
}

func getInt64ValuesEncoder(pageEncoding parquet.Encoding, typ *parquet.SchemaElement, store *dictStore) (valuesEncoder, error) {
	switch pageEncoding {
	case parquet.Encoding_PLAIN:
		return &int64PlainEncoder{}, nil
	case parquet.Encoding_DELTA_BINARY_PACKED:
		return &int64DeltaBPEncoder{}, nil
	case parquet.Encoding_RLE_DICTIONARY:
		return &dictEncoder{
			dictStore: *store,
		}, nil
	default:
		return nil, errors.Errorf("unsupported encoding %s for int64", pageEncoding)
	}
}

func getValuesEncoder(pageEncoding parquet.Encoding, typ *parquet.SchemaElement, store *dictStore) (valuesEncoder, error) {
	// Change the deprecated value
	if pageEncoding == parquet.Encoding_PLAIN_DICTIONARY {
		pageEncoding = parquet.Encoding_RLE_DICTIONARY
	}

	switch *typ.Type {
	case parquet.Type_BOOLEAN:
		return getBooleanValuesEncoder(pageEncoding, store)

	case parquet.Type_BYTE_ARRAY:
		return getByteArrayValuesEncoder(pageEncoding, store)

	case parquet.Type_FIXED_LEN_BYTE_ARRAY:
		if typ.TypeLength == nil {
			return nil, errors.Errorf("type %s with nil type len", typ.Type)
		}
		return getFixedLenByteArrayValuesEncoder(pageEncoding, int(*typ.TypeLength), store)

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
		return getInt32ValuesEncoder(pageEncoding, typ, store)

	case parquet.Type_INT64:
		return getInt64ValuesEncoder(pageEncoding, typ, store)

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
		return &byteArrayPlainEncoder{}, nil
	case parquet.Type_FIXED_LEN_BYTE_ARRAY:
		if typ.TypeLength == nil {
			return nil, errors.Errorf("type %s with nil type len", typ)
		}
		return &byteArrayPlainEncoder{length: int(*typ.TypeLength)}, nil
	case parquet.Type_FLOAT:
		return &floatPlainEncoder{}, nil
	case parquet.Type_DOUBLE:
		return &doublePlainEncoder{}, nil
	case parquet.Type_INT32:
		return &int32PlainEncoder{}, nil
	case parquet.Type_INT64:
		return &int64PlainEncoder{}, nil
	case parquet.Type_INT96:
		return &int96PlainEncoder{}, nil
	}

	return nil, errors.Errorf("type %s is not supported for dict value encoder", typ)
}

func writeChunk(ctx context.Context, w writePos, schema SchemaWriter, col *Column, codec parquet.CompressionCodec, pageFn newDataPageFunc, kvMetaData map[string]string) (*parquet.ColumnChunk, error) {
	pos := w.Pos() // Save the position before writing data
	chunkOffset := pos
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
		compSize, unCompSize, err := dict.write(ctx, w)
		if err != nil {
			return nil, err
		}
		totalComp = w.Pos() - pos
		// Header size plus the rLevel and dLevel size
		headerSize := totalComp - int64(compSize)
		totalUnComp = int64(unCompSize) + headerSize
		pos = w.Pos() // Move position for data pos
	}

	page := pageFn(useDict)

	if err := page.init(schema, col, codec); err != nil {
		return nil, err
	}

	compSize, unCompSize, err := page.write(ctx, w)
	if err != nil {
		return nil, err
	}

	totalComp += w.Pos() - pos
	// Header size plus the rLevel and dLevel size
	headerSize := totalComp - int64(compSize)
	totalUnComp += int64(unCompSize) + headerSize

	encodings := make([]parquet.Encoding, 0, 3)
	encodings = append(encodings,
		parquet.Encoding_RLE,
		col.data.encoding(),
	)
	if useDict {
		encodings[1] = parquet.Encoding_PLAIN // In dictionary we use PLAIN for the data, not the column encoding
		encodings = append(encodings, parquet.Encoding_RLE_DICTIONARY)
	}

	keyValueMetaData := make([]*parquet.KeyValue, 0, len(kvMetaData))
	for k, v := range kvMetaData {
		value := v
		keyValueMetaData = append(keyValueMetaData, &parquet.KeyValue{Key: k, Value: &value})
	}
	sort.Slice(keyValueMetaData, func(i, j int) bool {
		return keyValueMetaData[i].Key < keyValueMetaData[j].Key
	})

	nullCount := int64(col.data.values.nullValueCount())
	distinctCount := int64(col.data.values.numDistinctValues())

	stats := &parquet.Statistics{
		MinValue:      col.data.minValue(),
		MaxValue:      col.data.maxValue(),
		NullCount:     &nullCount,
		DistinctCount: &distinctCount,
	}

	ch := &parquet.ColumnChunk{
		FilePath:   nil, // No support for external
		FileOffset: chunkOffset,
		MetaData: &parquet.ColumnMetaData{
			Type:                  col.data.parquetType(),
			Encodings:             encodings,
			PathInSchema:          col.pathArray(),
			Codec:                 codec,
			NumValues:             int64(col.data.values.numValues() + col.data.values.nullValueCount()),
			TotalUncompressedSize: totalUnComp,
			TotalCompressedSize:   totalComp,
			KeyValueMetadata:      keyValueMetaData,
			DataPageOffset:        pos,
			IndexPageOffset:       nil,
			DictionaryPageOffset:  dictPageOffset,
			Statistics:            stats,
			EncodingStats:         nil,
		},
		OffsetIndexOffset: nil,
		OffsetIndexLength: nil,
		ColumnIndexOffset: nil,
		ColumnIndexLength: nil,
	}

	return ch, nil
}

func writeRowGroup(ctx context.Context, w writePos, schema SchemaWriter, codec parquet.CompressionCodec, pageFn newDataPageFunc, h *flushRowGroupOptionHandle) ([]*parquet.ColumnChunk, error) {
	dataCols := schema.Columns()
	var res = make([]*parquet.ColumnChunk, 0, len(dataCols))
	for _, ci := range dataCols {
		ch, err := writeChunk(ctx, w, schema, ci, codec, pageFn, h.getMetaData(ci.FlatName()))
		if err != nil {
			return nil, err
		}

		res = append(res, ch)
	}

	return res, nil
}
