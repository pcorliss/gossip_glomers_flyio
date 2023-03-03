package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"strconv"
	"time"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

type AddMessage struct {
	MsgType string `json:"type"`
	Delta   int    `json:"delta"`
}

func main() {
	n := maelstrom.NewNode()
	kv := maelstrom.NewSeqKV(n)
	ctx := context.TODO()

	lastWrite := "junk"

	n.Handle("read", func(msg maelstrom.Message) error {
		var err error
		var num int

		// Forcing a read of last written element to hopefully produce a later state of the sequential kv
		_, _ = kv.ReadInt(ctx, lastWrite)

		num, err = kv.ReadInt(ctx, "counter")
		if err != nil {
			fmt.Fprintf(os.Stderr, "Error: %v\n", err)
			num = 0
		} else {
			fmt.Fprintf(os.Stderr, "Read Num: %d\n", num)
		}

		var response map[string]any = make(map[string]any)
		response["type"] = "read_ok"
		response["value"] = num
		return n.Reply(msg, response)
	})

	n.Handle("add", func(msg maelstrom.Message) error {
		var body AddMessage
		var err error
		var num int

		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		fmt.Fprintf(os.Stderr, "Attempting Add: %d\n", body.Delta)

		success := false
		for !success && body.Delta > 0 {
			num, err = kv.ReadInt(ctx, "counter")
			if err != nil {
				fmt.Fprintf(os.Stderr, "Error: %v\n", err)
				num = 0
			}
			err = kv.CompareAndSwap(ctx, "counter", num, num+body.Delta, true)
			if err != nil {
				fmt.Fprintf(os.Stderr, "Error: %v\n", err)
			} else {
				success = true
			}
		}

		fmt.Fprintf(os.Stderr, "Sum %d\n", num+body.Delta)

		var ts = time.Now().UnixMilli()
		lastWrite = "junk_" + strconv.FormatInt(ts, 10)
		// This is a weird store that doesn't gaurantee state being linear so we can instead force it to gaurantee state by writing a unique value
		// https://github.com/jepsen-io/maelstrom/issues/39
		err = kv.Write(ctx, lastWrite, ts)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		}

		var response map[string]string = make(map[string]string)
		response["type"] = "add_ok"
		return n.Reply(msg, response)
	})

	if err := n.Run(); err != nil {
		log.Fatal(err)
	}
}
