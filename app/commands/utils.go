package commands

import (
	"errors"
	"fmt"
	"github.com/codecrafters-io/redis-starter-go/app/commands/resp"
	"github.com/codecrafters-io/redis-starter-go/app/store"
	"github.com/codecrafters-io/redis-starter-go/app/store/stream"
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

func parseXReadArgs(args []string) (keys, ids []string, block int64) {
	block = -1
	index := 0

	if len(args) >= 3 && strings.ToLower(args[0]) == "block" {
		blockVal, err := strconv.ParseInt(args[1], 10, 64)
		if err == nil {
			block = blockVal
		}

		for i, arg := range args {
			if strings.ToLower(arg) == "streams" {
				index = i + 1
				break
			}
		}
	} else {
		for i, arg := range args {
			if strings.ToLower(arg) == "streams" {
				index = i + 1
				break
			}
		}
	}

	if index > 0 && index < len(args) {
		keys, ids = splitArray(args[index:])
	}

	return keys, ids, block
}

func handleBlockingRead(keys, ids []string, store store.DataStore) (resp.Value, error) {
	notifyCh := make(chan stream.Notification, len(keys))

	subscriptions := subscribeToStreams(keys, store, notifyCh)
	defer func() {
		for _, stream := range subscriptions {
			stream.Unsubscribe(notifyCh)
		}
	}()

	<-notifyCh

	return readStreams(keys, ids, store)
}

func subscribeToStreams(keys []string, store store.DataStore, notifyCh chan stream.Notification) []*stream.Stream {
	subscriptions := make([]*stream.Stream, 0, len(keys))

	for _, streamKey := range keys {
		streamObj := store.Read(streamKey)
		if streamObj == nil {
			continue
		}

		stream, ok := streamObj.(*stream.Stream)
		if ok {
			stream.Subscribe(notifyCh)
			subscriptions = append(subscriptions, stream)
		}
	}

	return subscriptions
}

func readStreams(keys, ids []string, store store.DataStore) (resp.Value, error) {
	result := make([]resp.Value, 0, len(keys))

	for i, streamKey := range keys {
		streamObj := store.Read(streamKey)
		entryKey := ids[i]

		if streamObj == nil {
			return resp.BulkNullStringValue(), nil
		}

		stream, ok := streamObj.(*stream.Stream)
		if !ok {
			return resp.BulkNullStringValue(), nil
		}

		entries := stream.XRead(entryKey)
		if len(entries) == 0 {
			return resp.BulkNullStringValue(), nil
		}

		entriesValues := formatStreamEntries(entries)

		result = append(
			result,
			resp.ArrayValue(
				resp.BulkStringValue(streamKey),
				resp.ArrayValue(entriesValues...),
			),
		)
	}

	return resp.ArrayValue(result...), nil
}

func formatStreamEntries(entries []*stream.Entry) []resp.Value {
	entriesValues := make([]resp.Value, 0, len(entries))

	for _, entry := range entries {
		entryElements := make([]resp.Value, 0, 2*len(entry.Elements))

		for k, v := range entry.Elements {
			entryElements = append(
				entryElements,
				resp.BulkStringValue(k),
				resp.BulkStringValue(fmt.Sprintf("%v", v)),
			)
		}

		entriesValues = append(
			entriesValues,
			resp.ArrayValue(
				resp.BulkStringValue(entry.Id),
				resp.ArrayValue(entryElements...),
			),
		)
	}

	return entriesValues
}
