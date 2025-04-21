package commands

import (
	"errors"
	"slices"
	"strconv"
	"strings"
)

type SetCommandOptions struct {
	Key            string // The key provided in the command
	Value          string // The value to be set
	NX             bool   // Set if NX is provided
	XX             bool   // Set if XX is provided
	GET            bool   // Set if GET is provided
	ExpireSeconds  int64  // Use EX option (seconds), nil if not used
	ExpireMillis   int64  // Use PX option (milliseconds), nil if not used
	ExpireAtSec    int64  // Use EXAT (expiration in absolute seconds)
	ExpireAtMillis int64  // Use PXAT (expiration in absolute milliseconds)
	KeepTTL        bool   // Set if KEEPTTL is provided
}

func uppercasedCompare(a string) func(string) bool {
	return func(b string) bool {
		return strings.ToUpper(a) == strings.ToUpper(b)
	}
}

// Mildly inefficient parsing of command args for SET
// handles only key value, px at the moment
func parseSetCommandOptions(args []string) (SetCommandOptions, error) {
	var options SetCommandOptions

	if len(args) < 2 {
		return options, errors.New("not enough arguments")
	}

	var px int64

	pxExpiry := uppercasedCompare("px")

	if slices.ContainsFunc(args, pxExpiry) {
		idx := slices.IndexFunc(args, pxExpiry)
		if idx > -1 {
			c, err := strconv.Atoi(args[idx+1])

			if err != nil {
				return SetCommandOptions{}, err
			}

			px = int64(c)
		}
	}

	return SetCommandOptions{
		Key:           args[0],
		Value:         args[1],
		NX:            false, // Not implemented yet
		XX:            false,
		GET:           false,
		KeepTTL:       false,
		ExpireSeconds: 0,
		ExpireMillis:  px,
	}, nil
}

func Chunk[Slice ~[]T, T any](s Slice, size int) []Slice {
	var c []Slice

	for i := 0; i < len(s); i += size {
		end := min(size, len(s[i:]))
		c = append(c, s[i:i+end:i+end])
	}

	return c
}

func Zip[T, U any](a []T, b []U) []struct {
	First  T
	Second U
} {
	length := len(a)
	if len(b) < length {
		length = len(b)
	}

	result := make([]struct {
		First  T
		Second U
	}, length)
	for i := 0; i < length; i++ {
		result[i] = struct {
			First  T
			Second U
		}{a[i], b[i]}
	}
	return result
}

func splitArray(arr []string) (firstHalf []string, secondHalf []string) {
	middle := len(arr) / 2
	firstHalf = arr[:middle]
	secondHalf = arr[middle:]
	return
}
