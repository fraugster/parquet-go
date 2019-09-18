package cmds

import (
	"fmt"
	"io"
	"log"
	"os"

	goparquet "github.com/fraugster/parquet-go"
)

func catFile(address string, n int64) error {
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

	var total int64
	for {
		err := reader.ReadRowGroup()
		if err != nil && err != io.EOF {
			return fmt.Errorf("failed to read the row group: %q", err)
		}

		if err == io.EOF {
			return nil
		}

		for i := int64(0); i < reader.NumRecords(); i++ {
			if n > 0 && total >= n {
				return nil
			}
			total++
			data, err := reader.GetData()
			if err != nil {
				log.Printf("Reading data failed with error, skip current row group: %q", err)
				break
			}

			printData(data, "")
			fmt.Println()
		}
	}
}

func printPrimitive(ident, name string, v interface{}) {
	fmt.Println(ident + name + " = " + fmt.Sprint(v))
}

func printData(m map[string]interface{}, ident string) {
	for i := range m {
		switch t := m[i].(type) {
		case map[string]interface{}:
			fmt.Println(ident + i + ":")
			printData(t, ident+".")
		case []map[string]interface{}:
			for j := range t {
				fmt.Println(ident + i + ":")
				printData(t[j], ident+".")
			}
		case []byte:
			fmt.Println(ident + i + " = " + string(t))
		case [][]byte:
			for j := range t {
				fmt.Println(ident + i + " = " + string(t[j]))
			}
		case []interface{}:
			for j := range t {
				fmt.Println(ident + i + " = " + fmt.Sprint(t[j]))
			}
		default:
			printPrimitive(ident, i, t)
		}
	}
}
