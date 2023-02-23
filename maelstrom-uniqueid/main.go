package main

import (
	"fmt"
	"log"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

func main() {
	n := maelstrom.NewNode()

	var i int = 1

	n.Handle("generate", func(msg maelstrom.Message) error {
		var response map[string]string = make(map[string]string)
		response["type"] = "generate_ok"
		response["id"] = fmt.Sprintf("%s_%d", n.ID(), i)
		i += 1

		return n.Reply(msg, response)
	})

	if err := n.Run(); err != nil {
		log.Fatal(err)
	}
}
