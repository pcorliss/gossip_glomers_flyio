package main

import (
	"encoding/json"
	"fmt"
	"log"
	"os"
	"sync"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

type Txn struct {
	mode string
	key  int
	val  int
}

func (t *Txn) UnmarshalJSON(b []byte) error {
	a := []interface{}{&t.mode, &t.key, &t.val}
	return json.Unmarshal(b, &a)
}

type TxnMessage struct {
	MsgType string `json:"type"`
	MsgId   int    `json:"msg_id"`
	Txns    []Txn  `json:"txn"`
}

func main() {
	n := maelstrom.NewNode()
	store := make(map[int]int)
	storeMutex := sync.RWMutex{}

	n.Handle("txn", func(msg maelstrom.Message) error {
		var txns TxnMessage
		if err := json.Unmarshal(msg.Body, &txns); err != nil {
			fmt.Fprintf(os.Stderr, "ERROR: Failed to unmarshal %v\nERROR: %v\n", msg.Body, err)
			return err
		}

		var txnResults [][]any = make([][]any, len(txns.Txns))
		for idx, txn := range txns.Txns {
			switch txn.mode {
			case "r":
				storeMutex.RLock()
				txnResults[idx] = []any{txn.mode, txn.key, store[txn.key]}
				storeMutex.RUnlock()
			case "w":
				storeMutex.Lock()
				store[txn.key] = txn.val
				txnResults[idx] = []any{txn.mode, txn.key, txn.val}
				storeMutex.Unlock()
			}
		}

		for _, node := range n.NodeIDs() {
			if node == n.ID() {
				continue
			}
			n.Send(node, msg.Body)
		}

		var response map[string]any = make(map[string]any)
		response["type"] = "txn_ok"
		response["txn"] = txnResults
		return n.Reply(msg, response)
	})

	if err := n.Run(); err != nil {
		log.Fatal(err)
	}
}
