package cmds

import (
	"fmt"
	"log"
	"os"

	"github.com/spf13/cobra"
	goparquet "github.com/fraugster/parquet-go"
)

func init() {
	rootCmd.AddCommand(schemaCmd)
}

// TODO: add support for detailed schema (-d)

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
			// TODO: using fatal actually by-pass the defer, do it better
			log.Fatalf("Failed to read the parquet header: %q", err)
		}

		fmt.Print(reader.schemaReader.GetSchemaDefinition())
	},
}
