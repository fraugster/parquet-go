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
			// TODO: using fatal actually by-pass the defer, do it better
			log.Fatalf("Failed to read the parquet header: %q", err)
		}

		var total int64
		for {
			err := reader.ReadRowGroup()
			if err != nil && err != io.EOF {
				log.Fatalf("failed to read the row group: %q", err)
			}

			if err == io.EOF {
				break
			}

			total += reader.NumRecords()
		}

		fmt.Println("Total RowCount:", total)
	},
}
