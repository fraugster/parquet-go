package cmds

import (
	"log"
	"os"

	"github.com/spf13/cobra"
)

var recordCount *int64

func init() {
	recordCount = headCmd.PersistentFlags().Int64P("records", "n", 5, "The number of records to show")
	rootCmd.AddCommand(headCmd)
}

var headCmd = &cobra.Command{
	Use:   "head file-name.parquet",
	Short: "Prints the first n record of the Parquet file",
	Run: func(cmd *cobra.Command, args []string) {
		if len(args) != 1 {
			_ = cmd.Usage()
			os.Exit(1)
		}

		if err := catFile(os.Stdout, args[0], *recordCount); err != nil {
			log.Fatal(err)
		}
	},
}
