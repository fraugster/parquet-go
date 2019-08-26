package go_parquet

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

	w.AddColumn("foo", NewDataColumn(
		mustColumnStore(
			NewInt64Store(
				parquet.Encoding_PLAIN,
				true,
				&ColumnParameters{},
			),
		),
		parquet.FieldRepetitionType_REQUIRED,
	))
	w.AddColumn("bar", mustColumn(
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
	))
	w.AddColumn("baz", NewDataColumn(
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
	))
	w.AddColumn("quux", mustColumn(
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
	))

	expectedSchema :=
		`message msg {
  required int64 foo;
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

func mustColumn(c Column, err error) Column {
	if err != nil {
		panic(err)
	}
	return c
}
