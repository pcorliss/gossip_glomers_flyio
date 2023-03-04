package main

import (
	"encoding/json"
	"log"

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

func main() {
	n := maelstrom.NewNode()

	n.Handle("send", func(msg maelstrom.Message) error {
		var body SendMessage
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}
		var response map[string]any = make(map[string]any)
		response["type"] = "send_ok"
		response["offset"] = 1234 // Unique offset
		return n.Reply(msg, response)
	})

	n.Handle("poll", func(msg maelstrom.Message) error {
		var body OffsetsMessage
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}
		var response map[string]any = make(map[string]any)
		response["type"] = "poll_ok"
		response["msgs"] = map[string][][]int{
			"k1": {{1000, 9}, {10001, 5}},
			"k2": {{2000, 7}},
		}
		return n.Reply(msg, response)
	})

	n.Handle("commit_offsets", func(msg maelstrom.Message) error {
		var body OffsetsMessage
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
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
		var response map[string]any = make(map[string]any)
		response["type"] = "list_committed_offsets_ok"
		response["msgs"] = map[string]int{
			"k1": 1000,
			"k2": 2000,
		}
		return n.Reply(msg, response)
	})

	// 	fmt.Fprintf(os.Stderr, "Attempting Add: %d\n", body.Delta)
	// 	var ts = time.Now().UnixMilli()

	if err := n.Run(); err != nil {
		log.Fatal(err)
	}
}
