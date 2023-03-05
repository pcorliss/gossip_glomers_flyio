package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"strconv"
	"sync"
	"time"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

type SendMessage struct {
	MsgType string `json:"type"`
	Key     string `json:"key"`
	Msg     int    `json:"msg"`
}

type OffsetsMessage struct {
	MsgType string         `json:"type"`
	Offsets map[string]int `json:"offsets"`
}

type ListOffsetsMessage struct {
	MsgType string   `json:"type"`
	Keys    []string `json:"keys"`
}

func mutexKey(kv maelstrom.KV, key string, lock bool) {
	ctx := context.TODO()
	successfulWrite := false
	for !successfulWrite {
		err := kv.CompareAndSwap(ctx, key+"_lock", !lock, lock, true)
		if err != nil {
			var lockString = "Lock"
			if !lock {
				lockString = "Unlock"
			}
			fmt.Fprintf(os.Stderr, "WARNING: %s on %s failed. Sleeping for 1ms\n", lockString, key)
			time.Sleep(1 * time.Millisecond)
		} else {
			successfulWrite = true
		}
	}
}

func lockKey(kv maelstrom.KV, key string) {
	mutexKey(kv, key, true)
}

// Don't need to use the CaS operation because if we're locked we know nothing else can unlock it
func unlockKey(kv maelstrom.KV, key string) error {
	// mutexKey(kv, key, false)
	ctx := context.TODO()
	err := kv.Write(ctx, key+"_lock", false)
	if err != nil {
		fmt.Fprintf(os.Stderr, "ERROR: failed to unlock %s", key)
	}
	return err
}

func nextOffset(kv maelstrom.KV, key string) int {
	ctx := context.TODO()
	offset, err := kv.ReadInt(ctx, key+"_nextOffset")
	if err != nil {
		return 0
	}
	return offset
}

func appendVal(kv maelstrom.KV, cache map[string][]any, key string, val int) (int, error) {
	ctx := context.TODO()

	lockKey(kv, key)
	offset := nextOffset(kv, key)

	keyWithOffset := key + "_" + strconv.FormatInt(int64(offset), 10)
	err := kv.Write(ctx, keyWithOffset, val)
	if err != nil {
		fmt.Fprintf(os.Stderr, "ERROR: failed to write %s, %d\n%v\n", keyWithOffset, val, err)
		return 0, err
	}

	err = kv.Write(ctx, key+"_nextOffset", offset+1)
	if err != nil {
		fmt.Fprintf(os.Stderr, "ERROR: failed to write next offset. %s, %d\n%v\n", key, offset+1, err)
		return 0, err
	}

	if val != offset+1 {
		fmt.Fprintf(os.Stderr, "WARNING: Val %d and offset %d are potentially out of order for key %s\n", val, offset, key)
	}

	err = unlockKey(kv, key)
	if err != nil {
		return 0, err
	}
	return offset, nil
}

func getVals(kv maelstrom.KV, cache map[string][]any, key string, offset int) ([][]int, error) {
	ctx := context.TODO()

	vals := make([][]int, 0)

	nextOffset := nextOffset(kv, key)

	newCache := cache[key]
	if nextOffset > len(newCache) {
		newCache = make([]any, nextOffset)
		copy(newCache, cache[key])
		// for idx, v := range cache[key] {
		// 	newCache[idx] = v
		// }
	}
	for o := offset; o < nextOffset; o++ {
		if newCache[o] != nil {
			vals = append(vals, []int{o, newCache[o].(int)})
		} else {
			keyWithOffset := key + "_" + strconv.FormatInt(int64(o), 10)
			v, err := kv.ReadInt(ctx, keyWithOffset)
			if err != nil {
				fmt.Fprintf(os.Stderr, "ERROR: failed to read offset %d for key %s\n%v\n", o, keyWithOffset, err)
				return vals, err
			}
			vals = append(vals, []int{o, v})
			newCache[o] = v
		}
	}
	cache[key] = newCache

	return vals, nil
}

func getOffset(kv maelstrom.KV, key string) int {
	ctx := context.TODO()
	n, _ := kv.ReadInt(ctx, key+"_offset")
	return n
}

func setOffset(kv maelstrom.KV, key string, offset int) error {
	ctx := context.TODO()
	return kv.Write(ctx, key+"_offset", offset)
}

func main() {
	n := maelstrom.NewNode()
	kv := maelstrom.NewLinKV(n)
	cache := make(map[string][]any)
	cacheMutex := sync.RWMutex{}

	n.Handle("send", func(msg maelstrom.Message) error {
		var body SendMessage
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}
		offset, err := appendVal(*kv, cache, body.Key, body.Msg)

		// Update cache with new value
		cacheMutex.Lock()
		newCache := make([]any, offset+1)
		copy(newCache, cache[body.Key])
		newCache[offset] = body.Msg
		cache[body.Key] = newCache
		cacheMutex.Unlock()

		if err != nil {
			fmt.Fprintf(os.Stderr, "ERROR: failed to append val %d to %s\n", body.Msg, body.Key)
			return err
		}
		var response map[string]any = make(map[string]any)
		response["type"] = "send_ok"
		response["offset"] = offset // Unique offset
		return n.Reply(msg, response)
	})

	n.Handle("poll", func(msg maelstrom.Message) error {
		var body OffsetsMessage
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}
		var response map[string]any = make(map[string]any)
		response["type"] = "poll_ok"

		var messages = make(map[string][][]int)

		for key, offset := range body.Offsets {
			var err error
			cacheMutex.Lock()
			messages[key], err = getVals(*kv, cache, key, offset)
			cacheMutex.Unlock()
			if err != nil {
				fmt.Fprintf(os.Stderr, "ERROR: failed to poll for %s, %d\n", key, offset)
				return err
			}
		}

		response["msgs"] = messages
		return n.Reply(msg, response)
	})

	n.Handle("commit_offsets", func(msg maelstrom.Message) error {
		var body OffsetsMessage
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		for key, offset := range body.Offsets {
			err := setOffset(*kv, key, offset)
			if err != nil {
				fmt.Fprintf(os.Stderr, "ERROR: failed to commit offsets for %s, %d\n", key, offset)
				return err
			}
		}

		var response map[string]any = make(map[string]any)
		response["type"] = "commit_offsets_ok"
		return n.Reply(msg, response)
	})

	n.Handle("list_committed_offsets", func(msg maelstrom.Message) error {
		var body ListOffsetsMessage
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		var messages = make(map[string]int)
		for _, key := range body.Keys {
			messages[key] = getOffset(*kv, key)
		}

		var response map[string]any = make(map[string]any)
		response["type"] = "list_committed_offsets_ok"
		response["offsets"] = messages
		return n.Reply(msg, response)
	})

	// 	fmt.Fprintf(os.Stderr, "Attempting Add: %d\n", body.Delta)
	// 	var ts = time.Now().UnixMilli()

	if err := n.Run(); err != nil {
		log.Fatal(err)
	}
}
