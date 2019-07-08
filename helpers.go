package go_parquet

import "io"

type byteReader struct {
	io.Reader
}

func (br *byteReader) ReadByte() (byte, error) {
	buf := make([]byte, 1)
	n, err := br.Read(buf)
	if err != nil {
		return 0, err
	}
	if n == 0 {
		return 0, io.ErrUnexpectedEOF
	}

	return buf[0], nil
}

func decodeRLEValue(bytes []byte) int32 {
	switch len(bytes) {
	case 1:
		return int32(bytes[0])
	case 2:
		return int32(bytes[0]) + int32(bytes[1])<<8
	case 3:
		return int32(bytes[0]) + int32(bytes[1])<<8 + int32(bytes[2])<<16
	case 4:
		return int32(bytes[0]) + int32(bytes[1])<<8 + int32(bytes[2])<<16 + int32(bytes[3])<<24
	default:
		panic("invalid argument")
	}
}

func encodeRLEValue(in int32, size int) []byte {
	switch size {
	case 1:
		return []byte{byte(in & 255)}
	case 2:
		return []byte{
			byte(in & 255),
			byte((in >> 8) & 255),
		}
	case 3:
		return []byte{
			byte(in & 255),
			byte((in >> 8) & 255),
			byte((in >> 16) & 255),
		}
	case 4:
		return []byte{
			byte(in & 255),
			byte((in >> 8) & 255),
			byte((in >> 16) & 255),
			byte((in >> 24) & 255),
		}
	default:
		panic("invalid argument")
	}
}
