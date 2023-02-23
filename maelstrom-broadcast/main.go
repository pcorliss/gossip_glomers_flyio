package main

import (
	"encoding/json"
	"log"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

func main() {
	n := maelstrom.NewNode()

	var messages []int = make([]int, 0)

	// {
	// 	"type": "broadcast",
	// 	"message": 1000
	//   }
	// {
	// 	"type": "broadcast_ok"
	//   }
	n.Handle("broadcast", func(msg maelstrom.Message) error {
		var body map[string]any
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		messages = append(messages, int(body["message"].(float64)))

		var response map[string]string = make(map[string]string)
		response["type"] = "broadcast_ok"

		return n.Reply(msg, response)
	})
	// {
	// 	"type": "read"
	//   }
	//   {
	// 	"type": "read_ok",
	// 	"messages": [1, 8, 72, 25]
	//   }
	n.Handle("read", func(msg maelstrom.Message) error {
		var response map[string]any = make(map[string]any)
		response["type"] = "read_ok"
		response["messages"] = messages

		return n.Reply(msg, response)
	})
	// {
	// 	"type": "topology",
	// 	"topology": {
	// 	  "n1": ["n2", "n3"],
	// 	  "n2": ["n1"],
	// 	  "n3": ["n1"]
	// 	}
	//   }
	//   {
	// 	"type": "topology_ok"
	//   }
	n.Handle("topology", func(msg maelstrom.Message) error {
		var response map[string]string = make(map[string]string)
		response["type"] = "topology_ok"

		return n.Reply(msg, response)
	})

	if err := n.Run(); err != nil {
		log.Fatal(err)
	}
}
