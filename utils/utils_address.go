package utils

import (
	"strconv"
)

type Address = string

// Int2Addr converts an integer to an address string
func Int2Addr(id uint64) Address {
	return "0x" + strconv.FormatUint(id, 16)
}
