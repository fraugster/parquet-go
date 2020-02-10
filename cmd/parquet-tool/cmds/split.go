package cmds

import (
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"strings"

	"github.com/spf13/cobra"
	goparquet "github.com/fraugster/parquet-go"
	"github.com/fraugster/parquet-go/parquet"
)

var (
	partSize          *string
	targetFolder      *string
	rowGroupSize      *string
	compressionMethod *string
)

func init() {
	partSize = splitFile.PersistentFlags().StringP("file-size", "s", "100MB", "The target size of parquet files, it is not the *exact* size on the output")
	targetFolder = splitFile.PersistentFlags().StringP("target-folder", "t", "", "Target folder to write the files, use the source file folder if it's empty")
	rowGroupSize = splitFile.PersistentFlags().StringP("row-group-size", "r", "128MB", "Uncompressed row group size")
	compressionMethod = splitFile.PersistentFlags().StringP("compression", "c", "Snappy", "Compression method, valid values are Snappy, Gzip, None")
	rootCmd.AddCommand(splitFile)
}

var splitFile = &cobra.Command{
	Use:   "split file-name.parquet",
	Short: "Split the parquet file into multiple parquet files",
	Run: func(cmd *cobra.Command, args []string) {
		if len(args) != 1 {
			_ = cmd.Usage()
			os.Exit(1)
		}

		rgSize, err := humanToByte(*rowGroupSize)
		if err != nil {
			log.Fatalf("Invalid row group size: %q", *rowGroupSize)
		}

		pSize, err := humanToByte(*partSize)
		if err != nil {
			log.Fatalf("Invalid file size: %q", *partSize)
		}

		comp := parquet.CompressionCodec_UNCOMPRESSED
		switch strings.ToUpper(*compressionMethod) {
		case "SNAPPY":
			comp = parquet.CompressionCodec_SNAPPY
		case "GZIP":
			comp = parquet.CompressionCodec_GZIP
		case "NONE":
			comp = parquet.CompressionCodec_UNCOMPRESSED
		default:
			log.Fatalf("Invalid compression codec: %q", *rowGroupSize)
		}

		fl, err := os.Open(args[0])
		if err != nil {
			log.Fatalf("Can not open the file: %q", err)
		}
		defer fl.Close()

		reader, err := goparquet.NewFileReader(fl)
		if err != nil {
			log.Fatalf("could not create parquet reader: %q", err)
		}

		opts := []goparquet.FileWriterOption{
			goparquet.WithSchemaDefinition(reader.GetSchemaDefinition()),
			goparquet.WithCompressionCodec(comp),
			goparquet.WithMaxRowGroupSize(rgSize),
		}

		for i := 1; ; i++ {
			path := filepath.Join(*targetFolder, fmt.Sprintf("part_%d.parquet", i))
			ok, err := copyData(reader, path, pSize, opts...)
			if err != nil {
				log.Fatalf("Writing part failed: %q", err)
			}

			if ok {
				break
			}
		}
	},
}

func copyData(reader *goparquet.FileReader, path string, size int64, opts ...goparquet.FileWriterOption) (bool, error) {
	fl, err := os.Create(path)
	if err != nil {
		return false, err
	}
	defer fl.Close()

	writer := goparquet.NewFileWriter(fl, opts...)
	for {
		row, err := reader.NextRow()
		if err == io.EOF {
			return true, writer.Close()
		}
		if err != nil {
			return false, err
		}
		if err := writer.AddData(row); err != nil {
			return false, err
		}

		if writer.CurrentFileSize() >= size {
			return false, writer.Close()
		}
	}
}
