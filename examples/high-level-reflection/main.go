package main

import (
	"log"
	"reflect"

	goparquet "github.com/fraugster/parquet-go"
	"github.com/fraugster/parquet-go/floor"
	"github.com/fraugster/parquet-go/parquet"
	"github.com/fraugster/parquet-go/parquetschema"
)

func main() {
	schemaDef, err := parquetschema.ParseSchemaDefinition(
		`message test {
			required binary name (STRING);
			required binary data;
			required double score;
		}`)
	if err != nil {
		log.Fatalf("Parsing schema definition failed: %v", err)
	}

	parquetFilename := "output.parquet"

	fw, err := floor.NewFileWriter(parquetFilename,
		goparquet.WithSchemaDefinition(schemaDef),
		goparquet.WithCompressionCodec(parquet.CompressionCodec_SNAPPY),
	)
	if err != nil {
		log.Fatalf("Opening parquet file for writing failed: %v", err)
	}

	type record struct {
		Name  string  `parquet:"name"`
		Data  []byte  `parquet:"data"`
		Score float64 `parquet:"score"`
	}

	input := []record{
		{
			Name:  "Test",
			Data:  []byte{0xFF, 0x0A, 0x8E, 0x00, 0x12},
			Score: 23.5,
		},
	}

	for _, rec := range input {
		if err := fw.Write(rec); err != nil {
			log.Fatalf("Writing record failed: %v", err)
		}
	}

	if err := fw.Close(); err != nil {
		log.Fatalf("Closing parquet writer failed: %v", err)
	}

	fr, err := floor.NewFileReader(parquetFilename)
	if err != nil {
		log.Fatalf("Opening parquet file failed: %v", err)
	}

	var fileContent []record

	for fr.Next() {
		var rec record
		if err := fr.Scan(&rec); err != nil {
			log.Fatalf("Scanning record failed: %v", err)
		}
		fileContent = append(fileContent, rec)
	}

	equal := reflect.DeepEqual(input, fileContent)
	if equal {
		log.Printf("Congratulations! The input and the data read back are identical!")
	} else {
		log.Printf("This is strange... the data read back does not back what has been written to the file.")
	}
}
