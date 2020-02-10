package cmds

import (
	"log"
	"os"

	"github.com/spf13/cobra"
)

func init() {
	rootCmd.AddCommand(metaCmd)
}

var metaCmd = &cobra.Command{
	Use:   "meta file-name.parquet",
	Short: "print the metadata of the parquet file",
	Run: func(cmd *cobra.Command, args []string) {
		if len(args) != 1 {
			_ = cmd.Usage()
			os.Exit(1)
		}

		if err := metaFile(os.Stdout, args[0]); err != nil {
			log.Fatal(err)
		}
	},
}
