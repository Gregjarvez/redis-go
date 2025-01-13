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

	fn := uppercasedCompare("PX")

	if slices.ContainsFunc(args, fn) {
		idx := slices.IndexFunc(args, fn)
		if idx > -1 {
			c, err := strconv.Atoi(args[idx+1])

			if err != nil {
				return SetCommandOptions{}, err
			}

			px = int64(c)
		}
	}

	return SetCommandOptions{
		Key:            args[0],
		Value:          args[1],
		NX:             slices.Contains(args, "NX"),
		XX:             false,
		GET:            false,
		KeepTTL:        false,
		ExpireSeconds:  0,
		ExpireMillis:   0,
		ExpireAtSec:    0,
		ExpireAtMillis: px,
	}, nil
}
