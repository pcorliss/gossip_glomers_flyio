package main

import (
	"encoding/json"
	"fmt"
	"log"
	"os"
	"sync"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

func main() {
	n := maelstrom.NewNode()

	var messages []int = make([]int, 0)
	var messageSet map[int]bool = make(map[int]bool)
	var messageMutex = sync.RWMutex{}

	type Topology map[string][]string
	type TopologyMessage struct {
		MsgType  string   `json:"type"`
		Topology Topology `json:"topology"`
	}

	var topology Topology = nil

	n.Handle("broadcast_ok", func(msg maelstrom.Message) error { return nil })

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

		var response map[string]string = make(map[string]string)
		response["type"] = "broadcast_ok"

		var num int = int(body["message"].(float64))
		if messageSet[num] {
			// If we already have this number skip re-broadcasting
			return n.Reply(msg, response)
		}

		messageMutex.Lock()
		messages = append(messages, int(body["message"].(float64)))
		messageSet[num] = true
		messageMutex.Unlock()

		// rebroadcast to topology neighbors
		// but exclude the sender
		var neighbors = topology[n.ID()]
		for _, neighbor := range neighbors {
			if neighbor == msg.Src {
				continue
			}
			n.Send(neighbor, msg.Body)
			// n.RPC(neighbor, msg.Body, func(msg maelstrom.Message) error {
			// 	var neighborResponse map[string]string = make(map[string]string)
			// 	neighborResponse["type"] = "broadcast_ok"
			// 	return n.Reply(msg, neighborResponse)
			// })
		}

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
		messageMutex.RLock()
		response["messages"] = messages
		messageMutex.RUnlock()

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
		var body TopologyMessage
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		topology = body.Topology
		fmt.Fprintf(os.Stderr, "topology: %v\n", topology)

		var response map[string]string = make(map[string]string)
		response["type"] = "topology_ok"

		return n.Reply(msg, response)
	})

	if err := n.Run(); err != nil {
		log.Fatal(err)
	}
}
