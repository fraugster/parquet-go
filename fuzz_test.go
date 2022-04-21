package goparquet

import (
	"bytes"
	"fmt"
	"testing"
)

// this is meant to contain all unit tests that are testing crashers found through fuzz testing.

func TestFuzzThriftReadCrashes(t *testing.T) {
	crashers := []string{
		"PAR1)\xfa\xad\xa0\x93\xcd)000000000" +
			"00000000000\x1b\x00\x00\x00PAR1",
		"PAR1I\U000d7fd7\xef\xbf000000000" +
			"0000000000\x1b\x00\x00\x00PAR1",
		"PAR1I0t\x84\xd80\x010\x01'\x8a\x04\xd90\"\x01" +
			"'\x8a\x04\xfc\x0300e0Re0r\t\x04\xf6�\xef" +
			"\xbf0000000000000004\x00\x00\x00" +
			"PAR1",
		"PAR1)\xfa\xad\xa0\x93\xcd)000000000" +
			"0000000000\x1b\x00\x00\x00PAR1",
		"PAR1I\U000d7fd7\xef\xbf000000000" +
			"00000000000\x1b\x00\x00\x00PAR1",
		"PAR1I0t\x84\xd80\x010\x01'\x8a\x04\xd90\"\x01" +
			"'\x8a\x04\xfc\x0300\x0400\xb9\f\x04\x040\xb9\xf3\xfb\xfb\xce" +
			"\xb9\f000000000000004\x00\x00\x00" +
			"PAR1",
	}

	for idx, data := range crashers {
		t.Run(fmt.Sprintf("crasher%d", idx), func(t *testing.T) {
			r, err := NewFileReader(bytes.NewReader([]byte(data)))
			if err != nil {
				t.Logf("NewFileReader failed: %v", err)
				return
			}

			for {
				_, err := r.NextRow()
				if err != nil {
					break
				}
			}
		})
	}
}
