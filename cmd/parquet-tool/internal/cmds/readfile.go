package cmds

import (
	"fmt"
	"io"
	"log"
	"os"
	"strings"
	"text/tabwriter"

	goparquet "github.com/fraugster/parquet-go"
)

func catFile(w io.Writer, address string, n int64) error {
	fl, err := os.Open(address)
	if err != nil {
		return fmt.Errorf("can not open the file: %q", err)
	}
	defer fl.Close()

	reader, err := goparquet.NewFileReader(fl)
	if err != nil {
		// TODO: using fatal actually by-pass the defer, do it better
		return fmt.Errorf("failed to read the parquet header: %q", err)
	}

	for {
		data, err := reader.NextRow()
		if err == io.EOF {
			return nil
		}
		if err != nil {
			log.Printf("Reading data failed with error, skip current row group: %q", err)
			continue
		}

		printData(w, data, "")
		fmt.Println()
	}
}

func printPrimitive(w io.Writer, ident, name string, v interface{}) {
	_, _ = fmt.Fprintln(w, ident+name+" = "+fmt.Sprint(v))
}

func printData(w io.Writer, m map[string]interface{}, ident string) {
	for i := range m {
		switch t := m[i].(type) {
		case map[string]interface{}:
			_, _ = fmt.Fprintln(w, ident+i+":")
			printData(w, t, ident+".")
		case []map[string]interface{}:
			for j := range t {
				_, _ = fmt.Fprintln(w, ident+i+":")
				printData(w, t[j], ident+".")
			}
		case []byte:
			_, _ = fmt.Fprintln(w, ident+i+" = "+string(t))
		case [][]byte:
			for j := range t {
				_, _ = fmt.Fprintln(w, ident+i+" = "+string(t[j]))
			}
		case []interface{}:
			for j := range t {
				_, _ = fmt.Fprintln(w, ident+i+" = "+fmt.Sprint(t[j]))
			}
		default:
			printPrimitive(w, ident, i, t)
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
		// TODO: using fatal actually by-pass the defer, do it better
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
