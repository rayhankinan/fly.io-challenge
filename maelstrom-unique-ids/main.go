package main

import (
	"encoding/json"
	"log"

	"github.com/google/uuid"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

func main() {
	n := maelstrom.NewNode()

	n.Handle("generate", func(msg maelstrom.Message) error {
		// Unmarshal the body into a loosely typed map
		var inputBody map[string]interface{}
		if err := json.Unmarshal(msg.Body, &inputBody); err != nil {
			return err
		}

		// Update the message body to return back
		newBody := make(map[string]interface{})

		newBody["type"] = "generate_ok"
		newBody["id"] = uuid.New().String()

		// Reply the original message back with the updated body
		return n.Reply(msg, newBody)
	})

	if err := n.Run(); err != nil {
		log.Fatal(err)
	}
}
