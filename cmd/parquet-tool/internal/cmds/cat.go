package cmds

import (
	"fmt"
	"io"
	"log"
	"os"

	"github.com/spf13/cobra"
	goparquet "github.com/fraugster/parquet-go"
)

func init() {
	rootCmd.AddCommand(catCmd)
}

// TODO: add support for detailed schema (-d)

var catCmd = &cobra.Command{
	Use:   "cat file-name.parquet",
	Short: "Print the parquet file content",
	Run: func(cmd *cobra.Command, args []string) {
		if len(args) != 1 {
			_ = cmd.Usage()
			os.Exit(1)
		}
		fl, err := os.Open(args[0])
		if err != nil {
			log.Fatalf("Can not open the file: %q", err)
		}
		defer fl.Close()

		reader, err := goparquet.NewFileReader(fl)
		if err != nil {
			// TODO: using fatal actually by-pass the defer, do it better
			log.Fatalf("Failed to read the parquet header: %q", err)
		}

		for {
			err := reader.ReadRowGroup()
			if err != nil && err != io.EOF {
				log.Fatalf("Failed to read the row group: %q", err)
			}

			if err == io.EOF {
				return
			}

			for i := int64(0); i < reader.NumRecords(); i++ {
				data, err := reader.GetData()
				if err != nil {
					log.Printf("Reading data failed with error, skip current row group: %q", err)
					break
				}

				printData(data, "")
				fmt.Println()
			}
		}
	},
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
