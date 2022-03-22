package cmds

import (
	"fmt"
	"io"
	"log"
	"os"
	"sort"
	"strings"
	"text/tabwriter"

	goparquet "github.com/fraugster/parquet-go"
	"github.com/fraugster/parquet-go/parquetschema"
)

func catFile(w io.Writer, address string, n int) error {
	fl, err := os.Open(address)
	if err != nil {
		return fmt.Errorf("can not open the file: %q", err)
	}
	defer fl.Close()

	reader, err := goparquet.NewFileReader(fl)
	if err != nil {
		return fmt.Errorf("failed to read the parquet header: %q", err)
	}

	columnOrder := getColumnOrder(reader.GetSchemaDefinition())

	for i := 0; (n == -1) || i < n; i++ {
		data, err := reader.NextRow()
		if err == io.EOF {
			return nil
		}
		if err != nil {
			log.Printf("Reading data failed with error, skip current row group: %q", err)
			continue
		}

		printData(w, data, "", columnOrder)
		fmt.Println()
	}

	return nil
}

func getColumnOrder(schemaDef *parquetschema.SchemaDefinition) map[string]int {
	cols := getColumnList(schemaDef.RootColumn.Children, "")

	colOrder := map[string]int{}

	for idx, colName := range cols {
		colOrder[colName] = idx
	}

	return colOrder
}

func getColumnList(colDefs []*parquetschema.ColumnDefinition, prefix string) []string {
	cols := []string{}
	for _, col := range colDefs {
		cols = append(cols, prefix+col.SchemaElement.Name)
		if col.Children != nil {
			cols = append(cols, getColumnList(col.Children, col.SchemaElement.Name+".")...)
		}
	}
	return cols
}

func printPrimitive(w io.Writer, ident, name string, v any) {
	_, _ = fmt.Fprintln(w, ident+name+" = "+fmt.Sprint(v))
}

func printData(w io.Writer, m map[string]any, ident string, columnOrder map[string]int) {
	cols := []string{}

	for colName := range m {
		cols = append(cols, colName)
	}

	sort.Slice(cols, func(i, j int) bool {
		return columnOrder[ident+cols[i]] < columnOrder[ident+cols[j]]
	})

	for _, colName := range cols {
		switch t := m[colName].(type) {
		case map[string]any:
			_, _ = fmt.Fprintln(w, ident+colName+":")
			printData(w, t, ident+".", columnOrder)
		case []map[string]any:
			for j := range t {
				_, _ = fmt.Fprintln(w, ident+colName+":")
				printData(w, t[j], ident+".", columnOrder)
			}
		case []byte:
			_, _ = fmt.Fprintln(w, ident+colName+" = "+string(t))
		case [][]byte:
			for j := range t {
				_, _ = fmt.Fprintln(w, ident+colName+" = "+string(t[j]))
			}
		case []any:
			for j := range t {
				_, _ = fmt.Fprintln(w, ident+colName+" = "+fmt.Sprint(t[j]))
			}
		default:
			printPrimitive(w, ident, colName, t)
		}
	}
}

func metaFile(w io.Writer, address string) error {
	fl, err := os.Open(address)
	if err != nil {
		return fmt.Errorf("can not open the file: %q", err)
	}
	defer fl.Close()

	reader, err := goparquet.NewFileReader(fl)
	if err != nil {
		return fmt.Errorf("failed to read the parquet header: %q", err)
	}

	cols := reader.Columns()
	writer := tabwriter.NewWriter(w, 8, 8, 0, '\t', 0)
	printFlatSchema(writer, cols, 0)
	return writer.Flush()
}

func printFlatSchema(w io.Writer, cols []*goparquet.Column, lvl int) {
	dot := strings.Repeat(".", lvl)
	for _, column := range cols {
		_, _ = fmt.Fprintf(w, "%s%s:\t\t", dot, column.Name())
		_, _ = fmt.Fprintf(w, "%s ", column.RepetitionType().String())
		if column.DataColumn() {
			_, _ = fmt.Fprintf(w, "%s R:%d D:%d\n", column.Type().String(), column.MaxRepetitionLevel(), column.MaxDefinitionLevel())
			continue
		} else {
			_, _ = fmt.Fprintf(w, "F:%d\n", column.ChildrenCount())
			printFlatSchema(w, column.Children(), lvl+1)
		}
	}
}
