package floor

import (
	"reflect"
	"strings"
)

var fieldNameFunc = fieldNameToLower

func fieldNameToLower(field reflect.StructField) string {
	parquetStructTag, ok := field.Tag.Lookup("parquet")
	if !ok {
		return strings.ToLower(field.Name)
	}

	parquetStructTagFields := strings.Split(parquetStructTag, ",")

	return strings.TrimSpace(parquetStructTagFields[0])
}
