package main

import (
	"encoding/csv"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"log"
	"os"
	"sort"
	"strconv"
	"strings"
	"unicode/utf8"

	goparquet "github.com/fraugster/parquet-go"
	"github.com/fraugster/parquet-go/parquet"
	"github.com/fraugster/parquet-go/parquetschema"
)

var printLog = func(string, ...interface{}) {}

func main() {
	inputFile := flag.String("input", "", "CSV file input")
	typeHints := flag.String("typehints", "", "type hints to help derive parquet schema. A comma-separated list of type hints in the format <column_name>=<parquettype>; valid parquet types: "+strings.Join(validTypeList(), ", "))
	outputFile := flag.String("output", "", "output parquet file")
	rowgroupSize := flag.Int64("rowgroup-size", 0, "row group size in bytes; if value is 0, then the row group size is unbounded")
	compressionCodec := flag.String("compression", "snappy", "compression algorithm; allowed values: "+strings.Join(validCompressionCodecs(), ", "))
	delimiter := flag.String("delimiter", ",", "CSV field separator")
	verbose := flag.Bool("v", false, "enable verbose logging")
	flag.Parse()

	if *inputFile == "" {
		log.Fatalf("Empty input file parameter")
	}

	if *outputFile == "" {
		log.Fatalf("Empty output file parameter")
	}

	codec, err := lookupCompressionCodec(*compressionCodec)
	if err != nil {
		log.Fatalf("Invalid compression codec %q: %v", *compressionCodec, err)
	}

	var delimiterRune rune

	if *delimiter != "" {
		delimiterRune, _ = utf8.DecodeRuneInString(*delimiter)
		if delimiterRune == '\r' || delimiterRune == '\n' || delimiterRune == '\uFFFD' {
			log.Fatalf("Invalid CSV field separator %q", *delimiter)
		}
	}

	if *verbose {
		printLog = log.Printf
	}

	types, err := parseTypeHints(*typeHints)
	if err != nil {
		log.Fatalf("Parsing type hints failed: %v", err)
	}

	printLog("Opening %s...", *inputFile)

	f, err := os.Open(*inputFile)
	if err != nil {
		log.Fatalf("Couldn't open input file: %v", err)
	}

	csvReader := csv.NewReader(f)

	if *delimiter != "" {
		csvReader.Comma = delimiterRune
	}

	records, err := csvReader.ReadAll()
	if err != nil {
		log.Fatalf("Reading CSV content failed: %v", err)
	}

	f.Close()

	header := records[0]
	records = records[1:]

	printLog("Finished reading %s, got %d records", *inputFile, len(records))

	schema := &parquetschema.SchemaDefinition{
		RootColumn: &parquetschema.ColumnDefinition{
			SchemaElement: &parquet.SchemaElement{
				Name: "msg",
			},
		},
	}

	fieldHandlers := make([]func(string) (interface{}, error), len(header))

	for idx, field := range header {
		typ := types[field]
		if typ == "" {
			typ = "string"
			types[field] = typ
		}

		col := &parquetschema.ColumnDefinition{
			SchemaElement: &parquet.SchemaElement{},
		}
		col.SchemaElement.RepetitionType = parquet.FieldRepetitionTypePtr(parquet.FieldRepetitionType_OPTIONAL)
		col.SchemaElement.Name = field

		switch typ {
		case "string":
			col.SchemaElement.Type = parquet.TypePtr(parquet.Type_BYTE_ARRAY)
			col.SchemaElement.LogicalType = parquet.NewLogicalType()
			col.SchemaElement.LogicalType.STRING = &parquet.StringType{}
			col.SchemaElement.ConvertedType = parquet.ConvertedTypePtr(parquet.ConvertedType_UTF8)
			fieldHandlers[idx] = byteArrayHandler
		case "byte_array":
			col.SchemaElement.Type = parquet.TypePtr(parquet.Type_BYTE_ARRAY)
			fieldHandlers[idx] = byteArrayHandler
		case "boolean":
			col.SchemaElement.Type = parquet.TypePtr(parquet.Type_BOOLEAN)
			fieldHandlers[idx] = booleanHandler
		case "int8":
			col.SchemaElement.Type = parquet.TypePtr(parquet.Type_INT32)
			col.SchemaElement.LogicalType = parquet.NewLogicalType()
			col.SchemaElement.LogicalType.INTEGER = &parquet.IntType{BitWidth: 8, IsSigned: true}
			col.SchemaElement.ConvertedType = parquet.ConvertedTypePtr(parquet.ConvertedType_INT_8)
			fieldHandlers[idx] = intHandler(8)
		case "uint8":
			col.SchemaElement.Type = parquet.TypePtr(parquet.Type_INT32)
			col.SchemaElement.LogicalType = parquet.NewLogicalType()
			col.SchemaElement.LogicalType.INTEGER = &parquet.IntType{BitWidth: 8, IsSigned: false}
			col.SchemaElement.ConvertedType = parquet.ConvertedTypePtr(parquet.ConvertedType_UINT_8)
			fieldHandlers[idx] = uintHandler(8)
		case "int16":
			col.SchemaElement.Type = parquet.TypePtr(parquet.Type_INT32)
			col.SchemaElement.LogicalType = parquet.NewLogicalType()
			col.SchemaElement.LogicalType.INTEGER = &parquet.IntType{BitWidth: 16, IsSigned: true}
			col.SchemaElement.ConvertedType = parquet.ConvertedTypePtr(parquet.ConvertedType_INT_16)
			fieldHandlers[idx] = intHandler(16)
		case "uint16":
			col.SchemaElement.Type = parquet.TypePtr(parquet.Type_INT32)
			col.SchemaElement.LogicalType = parquet.NewLogicalType()
			col.SchemaElement.LogicalType.INTEGER = &parquet.IntType{BitWidth: 16, IsSigned: false}
			col.SchemaElement.ConvertedType = parquet.ConvertedTypePtr(parquet.ConvertedType_UINT_16)
			fieldHandlers[idx] = uintHandler(16)
		case "int32":
			col.SchemaElement.Type = parquet.TypePtr(parquet.Type_INT32)
			col.SchemaElement.LogicalType = parquet.NewLogicalType()
			col.SchemaElement.LogicalType.INTEGER = &parquet.IntType{BitWidth: 32, IsSigned: true}
			col.SchemaElement.ConvertedType = parquet.ConvertedTypePtr(parquet.ConvertedType_INT_32)
			fieldHandlers[idx] = intHandler(32)
		case "uint32":
			col.SchemaElement.Type = parquet.TypePtr(parquet.Type_INT32)
			col.SchemaElement.LogicalType = parquet.NewLogicalType()
			col.SchemaElement.LogicalType.INTEGER = &parquet.IntType{BitWidth: 32, IsSigned: false}
			col.SchemaElement.ConvertedType = parquet.ConvertedTypePtr(parquet.ConvertedType_UINT_32)
			fieldHandlers[idx] = uintHandler(32)
		case "int64":
			col.SchemaElement.Type = parquet.TypePtr(parquet.Type_INT64)
			col.SchemaElement.LogicalType = parquet.NewLogicalType()
			col.SchemaElement.LogicalType.INTEGER = &parquet.IntType{BitWidth: 64, IsSigned: true}
			col.SchemaElement.ConvertedType = parquet.ConvertedTypePtr(parquet.ConvertedType_INT_64)
			fieldHandlers[idx] = intHandler(64)
		case "uint64":
			col.SchemaElement.Type = parquet.TypePtr(parquet.Type_INT64)
			col.SchemaElement.LogicalType = parquet.NewLogicalType()
			col.SchemaElement.LogicalType.INTEGER = &parquet.IntType{BitWidth: 64, IsSigned: false}
			col.SchemaElement.ConvertedType = parquet.ConvertedTypePtr(parquet.ConvertedType_UINT_64)
			fieldHandlers[idx] = uintHandler(64)
		case "float":
			col.SchemaElement.Type = parquet.TypePtr(parquet.Type_FLOAT)
			fieldHandlers[idx] = floatHandler
		case "double":
			col.SchemaElement.Type = parquet.TypePtr(parquet.Type_DOUBLE)
			fieldHandlers[idx] = doubleHandler
		case "int":
			col.SchemaElement.Type = parquet.TypePtr(parquet.Type_INT64)
			col.SchemaElement.LogicalType = parquet.NewLogicalType()
			col.SchemaElement.LogicalType.INTEGER = &parquet.IntType{BitWidth: 64, IsSigned: true}
			col.SchemaElement.ConvertedType = parquet.ConvertedTypePtr(parquet.ConvertedType_INT_64)
			fieldHandlers[idx] = intHandler(64)
		case "json":
			col.SchemaElement.Type = parquet.TypePtr(parquet.Type_BYTE_ARRAY)
			col.SchemaElement.LogicalType = parquet.NewLogicalType()
			col.SchemaElement.LogicalType.JSON = &parquet.JsonType{}
			col.SchemaElement.ConvertedType = parquet.ConvertedTypePtr(parquet.ConvertedType_JSON)
			fieldHandlers[idx] = jsonHandler
		default:
			log.Fatalf("Unsupport type %q", typ)
		}

		schema.RootColumn.Children = append(schema.RootColumn.Children, col)
	}

	if err := schema.Validate(); err != nil {
		log.Fatalf("Internal error: derived schema does not validate: %v\nschema: %s", err, schema.String())
	}

	printLog("Derived parquet schema: %s", schema.String())

	of, err := os.OpenFile(*outputFile, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0644)
	if err != nil {
		log.Fatalf("Couldn't open output file: %v", err)
	}
	defer of.Close()

	writerOptions := []goparquet.FileWriterOption{
		goparquet.WithCreator("csv2parquet"),
		goparquet.WithSchemaDefinition(schema),
		goparquet.WithCompressionCodec(codec),
	}

	if *rowgroupSize > 0 {
		writerOptions = append(writerOptions, goparquet.WithMaxRowGroupSize(*rowgroupSize))
	}

	pqWriter := goparquet.NewFileWriter(of, writerOptions...)

	for recordIndex, record := range records {
		data := make(map[string]interface{})
		for idx, fieldName := range header {
			handler := fieldHandlers[idx]

			v, err := handler(record[idx])
			if err != nil {
				log.Fatalf("In input record %d, couldn't convert value %q to type %s: %v", recordIndex+1, record[idx], types[fieldName], err)
			}
			data[fieldName] = v
		}
		if err := pqWriter.AddData(data); err != nil {
			log.Fatalf("In input record %d, adding data failed: %v", recordIndex+1, err)
		}
	}

	if err := pqWriter.Close(); err != nil {
		log.Fatalf("Closing parquet writer failed: %v", err)
	}

	printLog("Finished generating output file %s", *outputFile)
}

func parseTypeHints(s string) (map[string]string, error) {
	typeMap := make(map[string]string)

	if s == "" {
		return typeMap, nil
	}

	hintsList := strings.Split(s, ",")
	for _, hint := range hintsList {
		hint = strings.TrimSpace(hint)

		hintFields := strings.Split(hint, "=")
		if len(hintFields) != 2 {
			return nil, fmt.Errorf("invalid type hint %q", hint)
		}

		if !isValidType(hintFields[1]) {
			return nil, fmt.Errorf("invalid parquet type %q", hintFields[1])
		}

		typeMap[hintFields[0]] = hintFields[1]
	}

	return typeMap, nil
}

var validTypes = map[string]bool{
	"boolean":    true,
	"int8":       true,
	"uint8":      true,
	"int16":      true,
	"uint16":     true,
	"int32":      true,
	"uint32":     true,
	"int64":      true,
	"uint64":     true,
	"float":      true,
	"double":     true,
	"byte_array": true,
	"string":     true,
	"int":        true,
	"json":       true,
	// TODO: support more data types
}

func validTypeList() []string {
	l := make([]string, 0, len(validTypes))
	for k := range validTypes {
		l = append(l, k)
	}
	sort.Strings(l)
	return l
}

var validCodecs = map[string]parquet.CompressionCodec{
	"none":   parquet.CompressionCodec_UNCOMPRESSED,
	"snappy": parquet.CompressionCodec_SNAPPY,
	"gzip":   parquet.CompressionCodec_GZIP,
}

func validCompressionCodecs() []string {
	l := make([]string, 0, len(validCodecs))
	for k := range validCodecs {
		l = append(l, k)
	}
	sort.Strings(l)
	return l
}

func lookupCompressionCodec(codec string) (parquet.CompressionCodec, error) {
	c, ok := validCodecs[codec]
	if !ok {
		return parquet.CompressionCodec_UNCOMPRESSED, errors.New("unsupported compression codec")
	}
	return c, nil
}

func isValidType(t string) bool {
	return validTypes[t]
}

func byteArrayHandler(s string) (interface{}, error) {
	return []byte(s), nil
}

func booleanHandler(s string) (interface{}, error) {
	return strconv.ParseBool(s)
}

func uintHandler(bitSize int) func(string) (interface{}, error) {
	return func(s string) (interface{}, error) {
		i, err := strconv.ParseUint(s, 10, bitSize)
		if err != nil {
			return nil, err
		}
		switch bitSize {
		case 8, 16, 32:
			return uint32(i), nil
		case 64:
			return i, nil
		default:
			return nil, fmt.Errorf("invalid bit size %d", bitSize)
		}
	}
}

func intHandler(bitSize int) func(string) (interface{}, error) {
	return func(s string) (interface{}, error) {
		i, err := strconv.ParseInt(s, 10, bitSize)
		if err != nil {
			return nil, err
		}
		switch bitSize {
		case 8, 16, 32:
			return int32(i), nil
		case 64:
			return int64(i), nil
		default:
			return nil, fmt.Errorf("invalid bit size %d", bitSize)
		}
	}
}

func floatHandler(s string) (interface{}, error) {
	f, err := strconv.ParseFloat(s, 32)
	return float32(f), err
}

func doubleHandler(s string) (interface{}, error) {
	f, err := strconv.ParseFloat(s, 64)
	return f, err
}

func jsonHandler(s string) (interface{}, error) {
	data := []byte(s)
	var obj interface{}
	if err := json.Unmarshal(data, &obj); err != nil {
		return nil, err
	}
	return data, nil
}
