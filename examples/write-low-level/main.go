package main

import (
	"log"
	"os"

	goparquet "github.com/fraugster/parquet-go"
	"github.com/fraugster/parquet-go/parquet"
	"github.com/fraugster/parquet-go/parquetschema"
)

func main() {
	f, err := os.OpenFile("output.parquet", os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0644)
	if err != nil {
		log.Fatalf("Opening output file failed: %v", err)
	}
	defer f.Close()

	schemaDef, err := parquetschema.ParseSchemaDefinition(
		`message test {
			required int64 id;
			required binary city (STRING);
			optional int64 population;
		}`)
	if err != nil {
		log.Fatalf("Parsing schema definition failed: %v", err)
	}

	fw := goparquet.NewFileWriter(f,
		goparquet.WithCompressionCodec(parquet.CompressionCodec_SNAPPY),
		goparquet.WithSchemaDefinition(schemaDef),
		goparquet.WithCreator("write-lowlevel"),
	)

	inputData := []struct {
		ID   int
		City string
		Pop  int
	}{
		{ID: 1, City: "Berlin", Pop: 3520031},
		{ID: 2, City: "Hamburg", Pop: 1787408},
		{ID: 3, City: "Munich", Pop: 1450381},
		{ID: 4, City: "Cologne", Pop: 1060582},
		{ID: 5, City: "Frankfurt", Pop: 732688},
	}

	for _, input := range inputData {
		if err := fw.AddData(map[string]any{
			"id":         int64(input.ID),
			"city":       []byte(input.City),
			"population": int64(input.Pop),
		}); err != nil {
			log.Fatalf("Failed to add input %v to parquet file: %v", input, err)
		}
	}

	if err := fw.Close(); err != nil {
		log.Fatalf("Closing parquet file writer failed: %v", err)
	}
}
