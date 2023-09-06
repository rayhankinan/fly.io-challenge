package main

import (
	"encoding/json"
	"log"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

func main() {
	n := maelstrom.NewNode()

	n.Handle("echo", func(msg maelstrom.Message) error {
		// Unmarshal the body into a struct
		var inputBody struct {
			Echo string `json:"echo"`
		}
		if err := json.Unmarshal(msg.Body, &inputBody); err != nil {
			return err
		}

		// Update the message body to return back
		newBody := struct {
			Type string `json:"type"`
			Echo string `json:"echo"`
		}{
			Type: "echo_ok",
			Echo: inputBody.Echo,
		}

		// Reply the original message back with the updated body
		return n.Reply(msg, newBody)
	})

	if err := n.Run(); err != nil {
		log.Fatal(err)
	}
}
