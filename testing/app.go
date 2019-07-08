package main

import (
	"bytes"
	"io"
	"log"
	"os"
)

func main() {
	buf := &bytes.Buffer{}

	n, err := io.Copy(buf, os.Stdin)
	if err != nil {
		log.Fatal(err)
	}
	log.Println("Read n bytes")

	target := &bytes.Buffer{}


}
