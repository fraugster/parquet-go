package cmds

import (
	"fmt"
	"log"
	"os"

	"github.com/spf13/cobra"
	goparquet "github.com/fraugster/parquet-go"
)

func init() {
	rootCmd.AddCommand(rowCountCmd)
}

var rowCountCmd = &cobra.Command{
	Use:   "rowcount file-name.parquet",
	Short: "Prints the count of rows in Parquet file",
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

		fmt.Println("Total RowCount:", reader.NumRows())
	},
}
