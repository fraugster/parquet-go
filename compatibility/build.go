//go:build ignore
// +build ignore

package main

import (
	"flag"
	"os"
	"strings"

	"github.com/fraugster/parquet-go/parquetschema"

	goparquet "github.com/fraugster/parquet-go"
	"github.com/fraugster/parquet-go/parquet"
)

func main() {
	var (
		file    string
		pq      string
		version string
		comp    string
	)
	flag.StringVar(&file, "json", "/data.json", "json file to load")
	flag.StringVar(&pq, "pq", "/data.pq", "pq to save")
	flag.StringVar(&version, "version", "v1", "Page v1 or Page v2 (v1 / v2)")
	flag.StringVar(&comp, "compression", "snappy", "compression method, snappy, gzip, none")

	flag.Parse()

	data, err := loadDataFromJson(file)
	if err != nil {
		panic(err)
	}

	var opts []goparquet.FileWriterOption
	switch strings.ToUpper(comp) {
	case "SNAPPY":
		opts = append(opts, goparquet.WithCompressionCodec(parquet.CompressionCodec_SNAPPY))
	case "GZIP":
		opts = append(opts, goparquet.WithCompressionCodec(parquet.CompressionCodec_GZIP))
	case "NONE":
		opts = append(opts, goparquet.WithCompressionCodec(parquet.CompressionCodec_UNCOMPRESSED))
	default:
		panic("invalid codec: " + comp)
	}

	switch strings.ToUpper(version) {
	case "V1":
		// it is default
	case "V2":
		opts = append(opts, goparquet.WithDataPageV2())
	default:
		panic("invalid version: " + version)
	}

	sc, err := parquetschema.ParseSchemaDefinition(schema)
	if err != nil {
		panic(err)
	}

	opts = append(opts, goparquet.WithSchemaDefinition(sc))

	fl, err := os.Create(pq)
	if err != nil {
		panic(err)
	}

	writer := goparquet.NewFileWriter(fl, opts...)
	for i := range data {
		if err := writer.AddData(data[i].toMap()); err != nil {
			panic(err)
		}
	}

	if err := writer.Close(); err != nil {
		panic(err)
	}
}
