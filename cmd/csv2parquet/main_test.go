package main

import (
	"bytes"
	"testing"

	goparquet "github.com/fraugster/parquet-go"
	"github.com/fraugster/parquet-go/parquet"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestParseTypeHints(t *testing.T) {
	tests := map[string]struct {
		Input          string
		ExpectedOutput map[string]string
		ExpectErr      bool
	}{
		"simple": {
			Input:          "foo=boolean,bar=string",
			ExpectedOutput: map[string]string{"foo": "boolean", "bar": "string"},
		},
		"simply-with-spaces": {
			Input: "   foo  =  boolean ,	bar=string	 ",
			ExpectedOutput: map[string]string{"foo": "boolean", "bar": "string"},
		},
		"empty": {
			Input:          "",
			ExpectedOutput: map[string]string{},
		},
		"invalid-type": {
			Input:     "foo=invalid-type",
			ExpectErr: true,
		},
		"invalid-field": {
			Input:     "foo=boolean=invalid",
			ExpectErr: true,
		},
	}

	for testName, tt := range tests {
		t.Run(testName, func(t *testing.T) {
			output, err := parseTypeHints(tt.Input)
			if tt.ExpectErr {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
				assert.Equal(t, tt.ExpectedOutput, output)
			}
		})
	}
}

func TestTypeHandlers(t *testing.T) {
	tests := map[string]struct {
		Input          string
		Func           func(string) (interface{}, error)
		ExpectedOutput interface{}
		ExpectErr      bool
	}{
		"byte-array":        {"hello", byteArrayHandler, []byte("hello"), false},
		"boolean-true":      {"true", booleanHandler, true, false},
		"boolean-false":     {"false", booleanHandler, false, false},
		"boolean-invalid":   {"invalid", booleanHandler, false, true},
		"bool-UPPERCASE":    {"TRUE", booleanHandler, true, false},
		"bool-num-1":        {"1", booleanHandler, true, false},
		"bool-num-0":        {"0", booleanHandler, false, false},
		"uint-32":           {"1234", uintHandler(32), uint32(1234), false},
		"uint-invalid":      {"hello!", uintHandler(32), 0, true},
		"uint-invalid-bits": {"1234", uintHandler(28), 0, true},
		"uint-64":           {"1000000000000", uintHandler(64), uint64(1000000000000), false},
		"int-32":            {"-1234", intHandler(32), int32(-1234), false},
		"int-invalid":       {"goodbye!", intHandler(32), 0, true},
		"int-invalid-bits":  {"1234", intHandler(42), 0, true},
		"int-64":            {"1000000000000", intHandler(64), int64(1000000000000), false},
		"float":             {"3.4", floatHandler, float32(3.4), false},
		"double":            {"4.2", doubleHandler, float64(4.2), false},
		"json-simple":       {`{"hello":"world"}`, jsonHandler, []byte(`{"hello":"world"}`), false},
		"json-invalid":      {`{"hello":"world`, jsonHandler, nil, true},
	}

	for testName, tt := range tests {
		t.Run(testName, func(t *testing.T) {
			output, err := tt.Func(tt.Input)
			if tt.ExpectErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
				require.Equal(t, tt.ExpectedOutput, output)
			}
		})
	}
}

func TestCreateColumn(t *testing.T) {
	tests := map[string]struct {
		Field                 string
		Type                  string
		ExpectErr             bool
		ExpectedType          parquet.Type
		ExpectedLogicalType   *parquet.LogicalType
		ExpectedConvertedType *parquet.ConvertedType
	}{
		"simple-boolean": {
			Field:        "foo",
			Type:         "boolean",
			ExpectErr:    false,
			ExpectedType: parquet.Type_BOOLEAN,
		},
		"simple-byte-array": {
			Field:        "foo",
			Type:         "byte_array",
			ExpectErr:    false,
			ExpectedType: parquet.Type_BYTE_ARRAY,
		},
		"simple-float": {
			Field:        "foo",
			Type:         "float",
			ExpectErr:    false,
			ExpectedType: parquet.Type_FLOAT,
		},
		"simple-double": {
			Field:        "foo",
			Type:         "double",
			ExpectErr:    false,
			ExpectedType: parquet.Type_DOUBLE,
		},
		"invalid-type": {
			Field:     "foo",
			Type:      "invalid",
			ExpectErr: true,
		},
		"string": {
			Field:                 "foo",
			Type:                  "string",
			ExpectErr:             false,
			ExpectedType:          parquet.Type_BYTE_ARRAY,
			ExpectedLogicalType:   &parquet.LogicalType{STRING: &parquet.StringType{}},
			ExpectedConvertedType: parquet.ConvertedTypePtr(parquet.ConvertedType_UTF8),
		},
	}

	for testName, tt := range tests {
		t.Run(testName, func(t *testing.T) {
			col, _, err := createColumn(tt.Field, tt.Type)
			if tt.ExpectErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
				require.Equal(t, tt.Field, col.SchemaElement.Name)
				require.Equal(t, tt.ExpectedType, *col.SchemaElement.Type)
				if tt.ExpectedLogicalType != nil {
					require.Equal(t, tt.ExpectedLogicalType, col.SchemaElement.LogicalType)
				}
				if tt.ExpectedConvertedType != nil {
					require.Equal(t, tt.ExpectedConvertedType, col.SchemaElement.ConvertedType)
				}
			}
		})
	}
}

func TestDeriveSchema(t *testing.T) {
	tests := map[string]struct {
		Header         []string
		Types          map[string]string
		ExpectErr      bool
		ExpectedSchema string
	}{
		"single-boolean": {
			Header:         []string{"foo"},
			Types:          map[string]string{"foo": "boolean"},
			ExpectedSchema: "message msg {\n  optional boolean foo;\n}\n",
		},
		"all-uints": {
			Header: []string{"a", "b", "c", "d"},
			Types:  map[string]string{"a": "uint8", "b": "uint16", "c": "uint32", "d": "uint64"},
			ExpectedSchema: `message msg {
  optional int32 a (INT(8, false));
  optional int32 b (INT(16, false));
  optional int32 c (INT(32, false));
  optional int64 d (INT(64, false));
}
`,
		},
		"all-ints": {
			Header: []string{"a", "b", "c", "d", "e"},
			Types:  map[string]string{"a": "int8", "b": "int16", "c": "int32", "d": "int64", "e": "int"},
			ExpectedSchema: `message msg {
  optional int32 a (INT(8, true));
  optional int32 b (INT(16, true));
  optional int32 c (INT(32, true));
  optional int64 d (INT(64, true));
  optional int64 e (INT(64, true));
}
`,
		},
		"string": {
			Header: []string{"x"},
			Types:  map[string]string{"x": "string"},
			ExpectedSchema: `message msg {
  optional binary x (STRING);
}
`,
		},
		"json": {
			Header: []string{"x"},
			Types:  map[string]string{"x": "json"},
			ExpectedSchema: `message msg {
  optional binary x (JSON);
}
`,
		},
		"default-type": {
			Header: []string{"foobar"},
			Types:  map[string]string{},
			ExpectedSchema: `message msg {
  optional binary foobar (STRING);
}
`,
		},
		"invalid-type": {
			Header:    []string{"foobar"},
			Types:     map[string]string{"foobar": "invalid"},
			ExpectErr: true,
		},
	}

	for testName, tt := range tests {
		t.Run(testName, func(t *testing.T) {
			schema, _, err := deriveSchema(tt.Header, tt.Types)
			if tt.ExpectErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
				require.Equal(t, tt.ExpectedSchema, schema.String())
			}
		})
	}
}

func TestWriteParquetData(t *testing.T) {
	tests := map[string]struct {
		Header         []string
		Types          map[string]string
		Records        [][]string
		ExpectErr      bool
		ExpectedSchema string
		ExpectedRows   []map[string]interface{}
	}{
		"simple": {
			Header: []string{"person", "age", "is_vampire"},
			Types:  map[string]string{"person": "string", "age": "int16", "is_vampire": "boolean"},
			Records: [][]string{
				{"Viago", "379", "true"},
				{"Vladislav", "862", "true"},
				{"Deacon", "183", "true"},
				{"Petyr", "8000", "true"},
				{"Nick", "28", "true"},
				{"Stu", "30", "false"},
			},
			ExpectedSchema: "message msg {\n  optional binary person (STRING);\n  optional int32 age (INT(16, true));\n  optional boolean is_vampire;\n}\n",
			ExpectedRows: []map[string]interface{}{
				{"person": []byte("Viago"), "age": int32(379), "is_vampire": true},
				{"person": []byte("Vladislav"), "age": int32(862), "is_vampire": true},
				{"person": []byte("Deacon"), "age": int32(183), "is_vampire": true},
				{"person": []byte("Petyr"), "age": int32(8000), "is_vampire": true},
				{"person": []byte("Nick"), "age": int32(28), "is_vampire": true},
				{"person": []byte("Stu"), "age": int32(30), "is_vampire": false},
			},
		},
		"invalid-type": {
			Header:    []string{"foo"},
			Types:     map[string]string{"foo": "invalid-type"},
			ExpectErr: true,
			Records: [][]string{
				{"asdf"},
			},
		},
		"not-enough-columns-in-records": {
			Header:    []string{"foo"},
			Types:     map[string]string{"foo": "string"},
			ExpectErr: true,
			Records: [][]string{
				{},
			},
		},
		"invalid-type-in-record": {
			Header:    []string{"foo"},
			Types:     map[string]string{"foo": "int64"},
			ExpectErr: true,
			Records: [][]string{
				{"invalid value"},
			},
		},
	}

	for testName, tt := range tests {
		t.Run(testName, func(t *testing.T) {
			buf := &bytes.Buffer{}

			err := writeParquetData(
				buf,
				tt.Header,
				tt.Types,
				tt.Records,
				"unit test",
				parquet.CompressionCodec_SNAPPY,
				150*1024*1024,
			)

			if tt.ExpectErr {
				require.Error(t, err)
				return
			}

			require.NoError(t, err)

			r := bytes.NewReader(buf.Bytes())

			pqReader, err := goparquet.NewFileReader(r)
			require.NoError(t, err)

			require.Equal(t, tt.ExpectedSchema, pqReader.GetSchemaDefinition().String())

			rows := []map[string]interface{}{}

			for i := int64(0); i < pqReader.NumRows(); i++ {
				data, err := pqReader.NextRow()
				require.NoError(t, err)
				rows = append(rows, data)
			}

			require.Equal(t, tt.ExpectedRows, rows)
		})
	}

}
