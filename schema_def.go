package go_parquet

import (
	"bytes"
	"fmt"
	"io"

	"github.com/fraugster/parquet-go/parquet"
)

// SchemaDefinition represents a valid textual schema definition.
type SchemaDefinition struct {
	col *column
}

// ParseSchemaDefinition parses a textual schema definition and returns
// an object, or an error if parsing has failed.
func ParseSchemaDefinition(schemaText string) (*SchemaDefinition, error) {
	p := newSchemaParser(schemaText)
	if err := p.parse(); err != nil {
		return nil, err
	}

	return &SchemaDefinition{
		col: p.root,
	}, nil
}

func (sd *SchemaDefinition) String() string {
	if sd.col == nil {
		return "message empty {\n}\n"
	}

	buf := new(bytes.Buffer)

	fmt.Fprintf(buf, "message %s {\n", sd.col.Name())

	printCols(buf, sd.col.children, 2)

	fmt.Fprintf(buf, "}\n")

	return buf.String()
}

// SubSchema returns the direct child of the current schema definition
// that matches the provided name. If no such child exists, nil is
// returned.
func (sd *SchemaDefinition) SubSchema(name string) *SchemaDefinition {
	for _, c := range sd.col.children {
		if c.name == name {
			return &SchemaDefinition{
				col: c,
			}
		}
	}
	return nil
}

// SchemaElement returns the schema element associated with the current
// schema definition. If no schema element is present, then nil is returned.
func (sd *SchemaDefinition) SchemaElement() *parquet.SchemaElement {
	if sd == nil {
		return nil
	}

	return sd.col.element
}

func printCols(w io.Writer, cols []*column, indent int) {
	for _, col := range cols {
		printIndent(w, indent)

		switch col.element.GetRepetitionType() {
		case parquet.FieldRepetitionType_REPEATED:
			fmt.Fprintf(w, "repeated")
		case parquet.FieldRepetitionType_OPTIONAL:
			fmt.Fprintf(w, "optional")
		case parquet.FieldRepetitionType_REQUIRED:
			fmt.Fprintf(w, "required")
		}
		fmt.Fprintf(w, " ")

		if col.element.Type == nil {
			fmt.Fprintf(w, "group %s", col.element.GetName())
			if col.element.ConvertedType != nil {
				fmt.Fprintf(w, " (%s)", getSchemaConvertedType(col.element.GetConvertedType()))
			}
			fmt.Fprintf(w, " {\n")
			printCols(w, col.children, indent+2)

			printIndent(w, indent)
			fmt.Fprintf(w, "}\n")
		} else {
			typ := getSchemaType(col.element.GetType())
			fmt.Fprintf(w, "%s %s", typ, col.element.GetName())
			if col.element.LogicalType != nil {
				fmt.Fprintf(w, " (%s)", getSchemaLogicalType(col.element.GetLogicalType()))
			}
			if col.element.FieldID != nil {
				fmt.Fprintf(w, " = %d", col.element.GetFieldID())
			}
			fmt.Fprintf(w, ";\n")
		}
	}
}

func printIndent(w io.Writer, indent int) {
	for i := 0; i < indent; i++ {
		fmt.Fprintf(w, " ")
	}
}

func getSchemaType(t parquet.Type) string {
	switch t {

	case parquet.Type_BYTE_ARRAY:
		return "binary"
	case parquet.Type_FLOAT:
		return "float"
	case parquet.Type_DOUBLE:
		return "double"
	case parquet.Type_BOOLEAN:
		return "boolean"
	case parquet.Type_INT32:
		return "int32"
	case parquet.Type_INT64:
		return "int64"
	case parquet.Type_INT96:
		return "int96"
	}
	return fmt.Sprintf("UT:%s", t)
}

func getSchemaConvertedType(t parquet.ConvertedType) string {
	switch t {
	case parquet.ConvertedType_UTF8:
		return "STRING"
	case parquet.ConvertedType_LIST:
		return "LIST"
	case parquet.ConvertedType_MAP:
		return "MAP"
	case parquet.ConvertedType_MAP_KEY_VALUE:
		return "MAP_KEY_VALUE"
	}
	return fmt.Sprintf("UC:%s", t)
}

func getSchemaLogicalType(t *parquet.LogicalType) string {
	switch {
	case t.IsSetSTRING():
		return "STRING"
	default:
		return "BUG(UNKNOWN)"
	}
}
