package cmds

import (
	"fmt"
	"log"
	"os"

	goparquet "github.com/fraugster/parquet-go"
	"github.com/spf13/cobra"
)

func init() {
	rootCmd.AddCommand(schemaCmd)
}

var schemaCmd = &cobra.Command{
	Use:   "schema file-name.parquet",
	Short: "Print the parquet file schema",
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
			log.Fatalf("Failed to read the parquet header: %q", err)
		}

		fmt.Print(reader.GetSchemaDefinition())
	},
}
