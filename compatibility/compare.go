//go:build ignore
// +build ignore

package main

import (
	"flag"
	"reflect"
)

func main() {
	var (
		file string
		pq   string
	)
	flag.StringVar(&file, "json", "/data.json", "json file to load")
	flag.StringVar(&pq, "pq", "/data.pq", "pq to save")

	flag.Parse()

	jData, err := loadDataFromJson(file)
	if err != nil {
		panic(err)
	}

	pqData, err := loadDataFromParquet(pq)
	if err != nil {
		panic(err)
	}

	if len(pqData) != len(jData) {
		panic("the len is not equal")
	}

	for i := range pqData {
		if !reflect.DeepEqual(*pqData[i], *jData[i]) {
			panic("not equal")
		}
	}
}
