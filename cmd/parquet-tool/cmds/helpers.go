package cmds

import (
	"fmt"
	"strconv"
	"strings"
)

var acceptableSuffix = map[string]int64{
	"KB":  1024,
	"KiB": 1000,
	"MB":  1024 * 1024,
	"MiB": 1000 * 1000,
	"GB":  1024 * 1024 * 1024,
	"GiB": 1000 * 1000 * 1000,
	"TB":  1024 * 1024 * 1024 * 1024,
	"TiB": 1000 * 1000 * 1000 * 1000,
	"PB":  1024 * 1024 * 1024 * 1024 * 1024,
	"PiB": 1000 * 1000 * 1000 * 1000 * 1000,
}

func humanToByte(in string) (int64, error) {
	in = strings.Trim(in, " \n\t")
	if b, err := strconv.ParseInt(in, 10, 0); err == nil {
		return b, nil
	}

	for i := range acceptableSuffix {
		if strings.HasSuffix(in, i) {
			in = strings.TrimSuffix(in, i)
			b, err := strconv.ParseInt(in, 10, 0)
			if err != nil {
				return 0, err
			}
			return b * acceptableSuffix[i], nil
		}
	}

	return 0, fmt.Errorf("invalid format")
}
