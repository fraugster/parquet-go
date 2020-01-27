package goparquet

import (
	"testing"

	"github.com/apache/thrift/lib/go/thrift"
	"github.com/stretchr/testify/require"
	"github.com/fraugster/parquet-go/parquet"
)

func TestCopyThrift(t *testing.T) {
	in := parquet.NewColumnChunk()
	in.MetaData = parquet.NewColumnMetaData()
	in.FilePath = thrift.StringPtr("path")

	res, err := copyThrift(in)
	require.NoError(t, err)
	require.IsType(t, in, res)

	require.Equal(t, *in.FilePath, *res.(*parquet.ColumnChunk).FilePath)
	require.NotNil(t, res.(*parquet.ColumnChunk).MetaData)
	require.Nil(t, res.(*parquet.ColumnChunk).OffsetIndexOffset)
}

func TestCopySchema(t *testing.T) {
	schema := `message foo {
			required int32 id;
			repeated binary str (STRING);
			required group xxx {
				optional int96 inner;
			}
			optional group example (LIST) {
				repeated group list {
					required group element {
						required binary attribute (STRING);
						optional int32 one;
					}
				}
			}
		}
	`

	def, err := ParseSchemaDefinition(schema)
	require.NoError(t, err)

	out, err := CopySchema(def)
	require.NoError(t, err)

	require.Equal(t, def.String(), out.String())
}
