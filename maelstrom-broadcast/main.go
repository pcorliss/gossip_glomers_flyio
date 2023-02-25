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

	var unacknowledged map[string][]int = make(map[string][]int)
	var unacknowledgedMutex = sync.RWMutex{}

	// n.Handle("broadcast_ok", func(msg maelstrom.Message) error { return nil })

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
		messageMutex.Lock()
		if messageSet[num] {
			// If we already have this number skip re-broadcasting
			messageMutex.Unlock()
			return n.Reply(msg, response)
		}

		messages = append(messages, num)
		messageSet[num] = true
		messageMutex.Unlock()

		// rebroadcast to topology neighbors
		for _, neighbor := range topology[n.ID()] {

			// Just broadcast to every other node except src and self
			// for _, neighbor := range n.NodeIDs() {
			if neighbor == msg.Src || neighbor == n.ID() {
				// but exclude the sender
				continue
			}

			// var unacknowledged map[string][]int = make(map[string][]int)
			// var unacknowledgedMutex = sync.RWMutex()

			unacknowledgedMutex.Lock()
			unacknowledged[neighbor] = append(unacknowledged[neighbor], num)
			unacknowledgedMutex.Unlock()

			// Relay the broadcast to our neighbor
			n.RPC(neighbor, msg.Body, broadcastHandlerConstructor(n, num, unacknowledged, &unacknowledgedMutex))
		}

		// Check if the source has unackd messages
		unacknowledgedMutex.RLock()
		var unackd []int = unacknowledged[msg.Src]
		unacknowledgedMutex.RUnlock()

		// Time to try flushing the queue
		if len(unackd) > 0 {
			flushUnacked(n, msg.Src, unacknowledged, &unacknowledgedMutex)
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

// Returns broadcast reply handler
func broadcastHandlerConstructor(n *maelstrom.Node, num int, unacknowledged map[string][]int, unacknowledgedMutex *sync.RWMutex) maelstrom.HandlerFunc {
	return func(msg maelstrom.Message) error {
		// Neighbor Replies back with the affirmative
		fmt.Fprintf(os.Stderr, "Reply Src: %s Dest: %s Msg: %s\n", msg.Src, msg.Dest, msg.Body)

		// Success From Dest!
		// Depop from map
		unacknowledgedMutex.Lock()
		var filtered []int = make([]int, 0)
		for _, unackNum := range unacknowledged[msg.Src] {
			if unackNum != num {
				filtered = append(filtered, unackNum)
			}
		}
		unacknowledged[msg.Src] = filtered
		unacknowledgedMutex.Unlock()

		// Time to try flushing the queue
		if len(filtered) > 0 {
			flushUnacked(n, msg.Src, unacknowledged, unacknowledgedMutex)
		}

		return nil
	}
}

func flushUnacked(n *maelstrom.Node, destNode string, unacknowledged map[string][]int, unacknowledgedMutex *sync.RWMutex) error {
	unacknowledgedMutex.RLock()
	var messageIds []int = unacknowledged[destNode]
	fmt.Fprintf(os.Stderr, "Flushing %d Messages for Dest: %s\n", len(messageIds), destNode)
	for _, num := range messageIds {
		var msg map[string]any = make(map[string]any)
		msg["type"] = "broadcast"
		msg["message"] = num
		n.RPC(destNode, msg, broadcastHandlerConstructor(n, num, unacknowledged, unacknowledgedMutex))
	}
	unacknowledgedMutex.RUnlock()
	return nil
}
