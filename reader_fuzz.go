//go:build gofuzz
// +build gofuzz

package goparquet

import (
	"bytes"
	"errors"
	"io"
)

func FuzzFileReader(data []byte) int {
	r, err := NewFileReader(bytes.NewReader(data))
	if err != nil {
		return 0
	}

	for {
		_, err := r.NextRow()
		if err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			return 0
		}
		for _, col := range r.Columns() {
			_ = col.Element()
		}
	}

	return 1
}
