package main

import (
	"encoding/json"
	"log"
	"sync"

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
	store := make(map[string][]int)
	storeMutex := sync.RWMutex{}
	commit := make(map[string]int)
	commitMutex := sync.RWMutex{}

	n.Handle("send", func(msg maelstrom.Message) error {
		var body SendMessage
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}
		storeMutex.Lock()
		store[body.Key] = append(store[body.Key], body.Msg)
		var offset int = len(store[body.Key]) - 1
		storeMutex.Unlock()
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
		storeMutex.RLock()

		var messages = make(map[string][][]int)

		for key, offset := range body.Offsets {
			var m = make([][]int, 0, len(store[key][offset:]))
			for idx, val := range store[key][offset:] {
				m = append(m, []int{idx + offset, val})
			}
			messages[key] = m
		}

		storeMutex.RUnlock()

		response["msgs"] = messages
		return n.Reply(msg, response)
	})

	n.Handle("commit_offsets", func(msg maelstrom.Message) error {
		var body OffsetsMessage
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		commitMutex.Lock()
		for key, offset := range body.Offsets {
			commit[key] = offset
		}
		commitMutex.Unlock()

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
		commitMutex.RLock()
		for _, key := range body.Keys {
			messages[key] = commit[key]
		}
		commitMutex.RUnlock()

		var response map[string]any = make(map[string]any)
		response["type"] = "list_committed_offsets_ok"
		response["msgs"] = messages
		return n.Reply(msg, response)
	})

	// 	fmt.Fprintf(os.Stderr, "Attempting Add: %d\n", body.Delta)
	// 	var ts = time.Now().UnixMilli()

	if err := n.Run(); err != nil {
		log.Fatal(err)
	}
}
