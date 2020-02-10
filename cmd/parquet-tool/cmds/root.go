package cmds

import (
	"log"

	"github.com/spf13/cobra"
)

var rootCmd = &cobra.Command{
	Use:   "parquet-tool",
	Short: "parquet-tool is a tool to work with parquet files in pure Go",
}

// Execute try to find and execute the command
func Execute() {
	if err := rootCmd.Execute(); err != nil {
		log.Fatalf("Failed to execute command: %q", err)
	}
}
