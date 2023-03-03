package main

import (
	"encoding/json"
	"fmt"
	"log"
	"os"
	"sort"
	"sync"
	"time"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

type UnackdMsg struct {
	LastSent int64
	Message  int
}
type Topology map[string][]string
type TopologyMessage struct {
	MsgType  string   `json:"type"`
	Topology Topology `json:"topology"`
}

type BatchMessage struct {
	MsgType string `json:"type"`
	Nums    []int  `json:"nums"`
}

func main() {
	n := maelstrom.NewNode()

	var messages []int = make([]int, 0)
	var messageSet map[int]bool = make(map[int]bool)
	var messageMutex = sync.RWMutex{}

	var topology Topology = nil

	var unacknowledged map[string][]UnackdMsg = make(map[string][]UnackdMsg)
	var unacknowledgedMutex = sync.RWMutex{}

	var batch []int = make([]int, 0)
	var batchMutex = sync.RWMutex{}

	n.Handle("batch", func(msg maelstrom.Message) error {
		var body BatchMessage
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}
		messageMutex.Lock()
		messages = append(messages, body.Nums...)
		for _, n := range body.Nums {
			if messageSet[n] {
				continue
			}
			messages = append(messages, n)
			messageSet[n] = true
		}
		messageMutex.Unlock()
		return nil
	})

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

		// n0 acts as a hub and batches requests in broadcast challenge #3e
		if n.ID() == "n0" {
			batchMutex.Lock()
			if len(batch) == 0 {
				go func() {
					time.Sleep(500 * time.Millisecond)
					batchMutex.Lock()
					fmt.Fprintf(os.Stderr, "Flushing Batch: %d - %v\n", len(batch), batch)
					var batchM map[string]any = make(map[string]any)
					batchM["type"] = "batch"
					batchM["nums"] = batch
					for _, dest := range topology[n.ID()] {
						n.Send(dest, batchM)
					}
					batch = make([]int, 0)
					batchMutex.Unlock()
				}()
			}
			batch = append(batch, num)
			batchMutex.Unlock()
			return n.Reply(msg, response)
		}

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
			unacknowledged[neighbor] = append(unacknowledged[neighbor], UnackdMsg{time.Now().UnixMilli(), num})
			unacknowledgedMutex.Unlock()

			// Relay the broadcast to our neighbor
			n.RPC(neighbor, msg.Body, broadcastHandlerConstructor(n, num, unacknowledged, &unacknowledgedMutex))
		}

		// Check if the source has unackd messages
		unacknowledgedMutex.RLock()
		var unackd []UnackdMsg = unacknowledged[msg.Src]
		unacknowledgedMutex.RUnlock()

		// Time to try flushing the queue
		if len(unackd) > 0 {
			flushUnacked(n, msg.Src, unacknowledged, &unacknowledgedMutex)
		}

		return n.Reply(msg, response)
	})

	n.Handle("read", func(msg maelstrom.Message) error {
		var response map[string]any = make(map[string]any)
		response["type"] = "read_ok"
		messageMutex.RLock()
		response["messages"] = messages
		messageMutex.RUnlock()

		return n.Reply(msg, response)
	})

	n.Handle("topology", func(msg maelstrom.Message) error {
		// Suggested Topology is a 2D grid, but that yields results which are too slow.
		// Need to build something with fewer hops to satisfy latency requirements

		var body TopologyMessage
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		var newTopology Topology = make(Topology)

		// Get list of nodes
		nodes := make([]string, 0, len(body.Topology))
		for k := range body.Topology {
			nodes = append(nodes, k)
			newTopology[k] = make([]string, 0)
		}
		// Need a stable sort
		sort.Strings(nodes)

		// Handles cases where length is not a perfect square
		// var groupSize int = (int)(math.sqrt((float64)(len(nodes))-1)) + 1
		// Group Size of 5 causes msgs-per-op that is too high. Setting this to 9 seems to give a good result
		var groupSize = 25
		// var leaderNodes []string = make([]string, 0)
		for i := 0; i < len(nodes); i += groupSize {
			var n = nodes[i]
			// leaderNodes = append(leaderNodes, n)
			for j := i + 1; j < i+groupSize && j < len(nodes); j++ {
				var c = nodes[j]
				newTopology[n] = append(newTopology[n], c)
				newTopology[c] = append(newTopology[c], n)
			}
		}

		// for _, n := range leaderNodes {
		// 	for _, o := range leaderNodes {
		// 		if o != n {
		// 			newTopology[n] = append(newTopology[n], o)
		// 		}
		// 	}
		// }

		// topology = body.Topology
		topology = newTopology
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
func broadcastHandlerConstructor(n *maelstrom.Node, num int, unacknowledged map[string][]UnackdMsg, unacknowledgedMutex *sync.RWMutex) maelstrom.HandlerFunc {
	return func(msg maelstrom.Message) error {
		// Neighbor Replies back with the affirmative
		fmt.Fprintf(os.Stderr, "Reply Src: %s Dest: %s Msg: %s\n", msg.Src, msg.Dest, msg.Body)

		// Success From Dest!
		// Depop from map
		unacknowledgedMutex.Lock()
		var filtered []UnackdMsg = make([]UnackdMsg, 0)
		for _, unackMsg := range unacknowledged[msg.Src] {
			if unackMsg.Message != num {
				filtered = append(filtered, unackMsg)
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

func flushUnacked(n *maelstrom.Node, destNode string, unacknowledged map[string][]UnackdMsg, unacknowledgedMutex *sync.RWMutex) error {
	unacknowledgedMutex.Lock()
	var unackMsgs []UnackdMsg = unacknowledged[destNode]
	fmt.Fprintf(os.Stderr, "Unackd Messages - %d for Dest: %s\nMessages: %v\n", len(unackMsgs), destNode, unackMsgs)
	var updatedMsgs []UnackdMsg = make([]UnackdMsg, len(unackMsgs))
	var counter = 0
	var messages []int = make([]int, 0)
	for i, unackM := range unackMsgs {
		// Skip sending if it hasn't yet been 100ms since sending this message
		updatedMsgs[i] = unackM
		if time.Now().UnixMilli()-unackM.LastSent < 1000 {
			continue
		}
		updatedMsgs[i].LastSent = time.Now().UnixMilli()
		counter += 1
		var msg map[string]any = make(map[string]any)
		msg["type"] = "broadcast"
		msg["message"] = unackM.Message
		messages = append(messages, unackM.Message)
		n.RPC(destNode, msg, broadcastHandlerConstructor(n, unackM.Message, unacknowledged, unacknowledgedMutex))
	}
	unacknowledged[destNode] = updatedMsgs
	fmt.Fprintf(os.Stderr, "Updated Map: %v\n", updatedMsgs)
	unacknowledgedMutex.Unlock()
	fmt.Fprintf(os.Stderr, "Flushed %d Messages for Dest: %s - %v\n", counter, destNode, messages)
	var msgSet = make(map[int]bool)
	for _, message := range messages {
		if msgSet[message] {
			fmt.Fprintf(os.Stderr, "Exiting: Found duplicate message in flush output, %d\n", message)
			os.Exit(1)
		}
		msgSet[message] = true
	}
	return nil
}
