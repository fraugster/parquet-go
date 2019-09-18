package cmds

import (
	"log"

	"github.com/spf13/cobra"
)

var rootCmd = &cobra.Command{
	Use:   "parquet-tool",
	Short: "parquet-tool is a copy of parquet-tool in pure Go",
}

func Execute() {
	if err := rootCmd.Execute(); err != nil {
		log.Fatalf("Failed to execute command: %q", err)
	}
}
