package goparquet

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestParquetTesting(t *testing.T) {
	testingRoot := os.Getenv("PARQUET_TESTING_ROOT") // path where https://github.com/apache/parquet-testing has been cloned to.
	if testingRoot == "" {
		t.Skip("PARQUET_TESTING_ROOT not set, skipping test")
	}

	testFiles := []string{
		"data/alltypes_dictionary.parquet",
		"data/alltypes_plain.parquet",
		"data/alltypes_plain.snappy.parquet",
		"data/binary.parquet",
		"data/byte_array_decimal.parquet",
		"data/datapage_v2.snappy.parquet",
		"data/delta_binary_packed.parquet",
		//"data/delta_byte_array.parquet",
		"data/delta_encoding_optional_column.parquet",
		"data/delta_encoding_required_column.parquet",
		// "data/dict-page-offset-zero.parquet",
		"data/fixed_length_decimal.parquet",
		"data/fixed_length_decimal_legacy.parquet",
		// "data/hadoop_lz4_compressed.parquet", // LZ4 is currently unsupported out of the box.
		// "data/hadoop_lz4_compressed_larger.parquet",
		"data/int32_decimal.parquet",
		"data/int64_decimal.parquet",
		"data/list_columns.parquet",
		"data/nested_lists.snappy.parquet",
		"data/nested_maps.snappy.parquet",
		// "data/nested_structs.rust.parquet", // uses ZSTD which is currently unsupported out of the box.
		"data/nonnullable.impala.parquet",
		"data/nullable.impala.parquet",
		"data/nulls.snappy.parquet",
		"data/repeated_no_annotation.parquet",
	}

	for _, file := range testFiles {
		t.Run(file, func(t *testing.T) {
			fullPath := filepath.Join(testingRoot, file)

			f, err := os.Open(fullPath)
			require.NoError(t, err)
			defer f.Close()

			r, err := NewFileReader(f)
			require.NoError(t, err)

			numRows := r.NumRows()

			t.Logf("%s: got %d rows", file, numRows)
			t.Logf("%s: schema = %s", file, r.GetSchemaDefinition().String())

			for i := int64(0); i < numRows; i++ {
				_, err := r.NextRow()
				require.NoError(t, err)
			}
		})
	}
}
