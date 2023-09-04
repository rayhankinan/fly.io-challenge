package main

import (
	"encoding/json"
	"log"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

func main() {
	n := maelstrom.NewNode()

	n.Handle("echo", func(msg maelstrom.Message) error {
		// Unmarshal the body into a loosely typed map
		var inputBody map[string]interface{}
		if err := json.Unmarshal(msg.Body, &inputBody); err != nil {
			return err
		}

		// Update the message body to return back
		newBody := make(map[string]interface{})

		newBody["type"] = "echo_ok"
		newBody["echo"] = inputBody["echo"]

		// Reply the original message back with the updated body
		return n.Reply(msg, newBody)
	})

	if err := n.Run(); err != nil {
		log.Fatal(err)
	}
}
