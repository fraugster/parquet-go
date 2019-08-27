package goparquet

import (
	"encoding/csv"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/stretchr/testify/require"
)

func toCustomerMap(t *testing.T, data []string) map[string]interface{} {
	require.Len(t, data, 8)
	res := make(map[string]interface{})
	var err error
	cKey, err := strconv.ParseInt(data[0], 10, 0)
	if err == nil {
		res["c_custkey"] = cKey
	}

	res["c_name"] = []byte(data[1])
	res["c_address"] = []byte(data[2])
	cKey, err = strconv.ParseInt(data[3], 10, 0)
	if err == nil {
		res["c_nationkey"] = int32(cKey)
	}
	res["c_phone"] = []byte(data[4])
	fl, err := strconv.ParseFloat(data[5], 64)
	if err == nil {
		res["c_acctbal"] = fl
	}
	res["c_mktsegment"] = []byte(data[6])
	res["c_comment"] = []byte(data[7])

	return res
}

func customerMapTest(parquet, csvFl string) func(t *testing.T) {
	return func(t *testing.T) {
		f, err := os.Open(parquet)
		require.NoError(t, err)
		defer f.Close()

		f2, err := os.Open(csvFl)
		require.NoError(t, err)
		defer f2.Close()

		r := csv.NewReader(f2)
		r.Comma = '|'

		reader, err := NewFileReader(f)
		require.NoError(t, err)

		for {
			if err := reader.ReadRowGroup(); err == io.EOF {
				break
			}
			count := reader.NumRecords()
			for i := int64(0); i < count; i++ {
				rec, err := r.Read()
				require.NoError(t, err)
				read, err := reader.GetData()
				require.NoError(t, err)
				csvData := toCustomerMap(t, rec)
				assert.Equal(t, csvData, read)
			}
		}
	}
}

func TestCompatibility(t *testing.T) {
	root := os.Getenv("PARQUET_COMPATIBILITY_REPO_ROOT")
	if root == "" {
		t.Skip("The PARQUET_COMPATIBILITY_REPO_ROOT is missing, skip the tests")
	}

	for _, v := range []string{"NONE", "GZIP", "SNAPPY"} {
		pq := filepath.Join(root, "parquet-testdata", "impala", fmt.Sprintf("1.1.1-%s", v), "customer.impala.parquet")
		cs := filepath.Join(root, "parquet-testdata", "tpch", "customer.csv")
		t.Run(fmt.Sprintf("Customer %s", v), customerMapTest(pq, cs))
	}
}
