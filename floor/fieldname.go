package floor

import (
	"reflect"
	"strings"
)

var fieldNameFunc = fieldNameToLower

// TODO: replace this with a more sophisticated implementation,
// possibly involving the struct tag.
func fieldNameToLower(field reflect.StructField) string {
	return strings.ToLower(field.Name)
}
