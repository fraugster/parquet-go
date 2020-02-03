package parquetschema

import (
	"io/ioutil"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
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

func TestNilSchemaDef(t *testing.T) {
	var sd *SchemaDefinition

	require.Equal(t, "message empty {\n}\n", sd.String())

	sd = &SchemaDefinition{}

	require.Equal(t, "message empty {\n}\n", sd.String())
}

func TestInvalidSchema(t *testing.T) {
	_, err := ParseSchemaDefinition("")
	require.Error(t, err)
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
