package goparquet

import (
	"bytes"
	"io/ioutil"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/fraugster/parquet-go/parquet"
)

func TestSchemaDefinition(t *testing.T) {
	dir, err := os.Open("schema-files")
	require.NoError(t, err)

	files, err := dir.Readdirnames(-1)
	require.NoError(t, err)

	for idx, file := range files {
		fileContent, err := ioutil.ReadFile(filepath.Join("schema-files", file))
		require.NoError(t, err)

		schemaText := string(fileContent)

		sd, err := ParseSchemaDefinition(schemaText)
		require.NoError(t, err, "%d. %s: parsing schema definition returned error", idx, file)

		require.Equal(t, schemaText, sd.String(), "%d. %s: sd.String returned different string representation", idx, file)
	}
}

func TestSchemasPutTogetherManually(t *testing.T) {
	buf := new(bytes.Buffer)

	w := NewFileWriter(buf)

	require.NoError(t, w.AddColumn("foo", NewDataColumn(
		mustColumnStore(
			NewInt64Store(
				parquet.Encoding_PLAIN,
				true,
				&ColumnParameters{
					LogicalType: &parquet.LogicalType{
						INTEGER: &parquet.IntType{
							BitWidth: 64,
							IsSigned: true,
						},
					},
				},
			),
		),
		parquet.FieldRepetitionType_REQUIRED,
	)))
	require.NoError(t, w.AddColumn("bar", mustColumn(
		NewListColumn(
			NewDataColumn(
				mustColumnStore(
					NewInt32Store(
						parquet.Encoding_PLAIN,
						true,
						&ColumnParameters{},
					),
				),
				parquet.FieldRepetitionType_OPTIONAL,
			),
			parquet.FieldRepetitionType_OPTIONAL,
		),
	)))
	require.NoError(t, w.AddColumn("baz", NewDataColumn(
		mustColumnStore(
			NewByteArrayStore(
				parquet.Encoding_PLAIN,
				true,
				&ColumnParameters{
					LogicalType: &parquet.LogicalType{
						STRING: parquet.NewStringType(),
					},
					FieldID: int32Ptr(23),
				},
			),
		),
		parquet.FieldRepetitionType_REQUIRED,
	)))
	require.NoError(t, w.AddColumn("quux", mustColumn(
		NewMapColumn(
			NewDataColumn(
				mustColumnStore(
					NewByteArrayStore(
						parquet.Encoding_PLAIN,
						true,
						&ColumnParameters{
							LogicalType: &parquet.LogicalType{
								STRING: parquet.NewStringType(),
							},
						},
					),
				),
				parquet.FieldRepetitionType_REQUIRED,
			),
			NewDataColumn(
				mustColumnStore(
					NewFixedByteArrayStore(
						parquet.Encoding_PLAIN,
						true,
						&ColumnParameters{
							LogicalType: &parquet.LogicalType{
								UUID: parquet.NewUUIDType(),
							},
							TypeLength: int32Ptr(16),
						},
					),
				),
				parquet.FieldRepetitionType_OPTIONAL,
			),
			parquet.FieldRepetitionType_REQUIRED,
		),
	)))

	expectedSchema :=
		`message msg {
  required int64 foo (INT(64, true));
  optional group bar (LIST) {
    repeated group list {
      optional int32 element;
    }
  }
  required binary baz (STRING) = 23;
  required group quux (MAP) {
    repeated group key_value (MAP_KEY_VALUE) {
      required binary key (STRING);
      optional fixed_len_byte_array(16) value (UUID);
    }
  }
}
`

	require.Equal(t, expectedSchema, w.GetSchemaDefinition().String(), "expected and produced schema definition does not match")
	t.Logf("generated schema: %s", w.GetSchemaDefinition().String())
}

func TestParseAndGenerateSchema(t *testing.T) {
	schema := `message msg {
  required int64 foo (INT(64, true));
  required int32 bar (DECIMAL(5, 3));
  required binary baz (JSON);
  required binary quux (BSON);
  required fixed_len_byte_array(16) bla (UUID);
  required binary fasel (ENUM);
  required int64 t1 (TIMESTAMP(NANOS, true));
  required int64 t2 (TIMESTAMP(MICROS, false));
  required int64 t3 (TIMESTAMP(MILLIS, true));
  required float f;
  required double d;
  required binary aa (UTF8);
  required int32 bb (TIME_MILLIS);
  required int64 cc (TIME_MICROS);
  required int64 dd (TIMESTAMP_MILLIS);
  required int64 ee (TIMESTAMP_MICROS);
  required int32 ff (UINT_8);
  required int32 gg (UINT_16);
  required int32 hh (UINT_32);
  required int64 ii (UINT_64);
  required int32 jj (INT_8);
  required int32 kk (INT_16);
  required int32 ll (INT_32);
  required int64 mm (INT_64);
  required fixed_len_byte_array(12) nn (INTERVAL);
}
`

	schemaDef, err := ParseSchemaDefinition(schema)
	require.NoError(t, err, "parsing schema definition failed")

	require.Equal(t, schema, schemaDef.String(), "expected and actual schema definition does not match")
}

func mustColumn(c *Column, err error) *Column {
	if err != nil {
		panic(err)
	}
	return c
}
