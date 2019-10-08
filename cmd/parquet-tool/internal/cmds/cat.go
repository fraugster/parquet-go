package cmds

import (
	"log"
	"os"

	"github.com/spf13/cobra"
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

		if err := catFile(os.Stdout, args[0], -1); err != nil {
			log.Fatal(err)
		}
	},
}
