package go_parquet

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
