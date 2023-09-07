package main

import (
	"context"
	"encoding/json"
	"log"
	"sync"
	"time"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

type PropagateBody struct {
	Type      string `json:"type"`
	Message   int64  `json:"message"`
	Timestamp int64  `json:"timestamp"`
}

func main() {
	// Create a new node
	n := maelstrom.NewNode()

	// Create a counter to store the values
	counter := &Counter{
		arr:           []Element{},
		readWriteLock: new(sync.RWMutex),
	}

	// Create a topology to store the nodes
	topology := []string{}

	n.Handle("broadcast", func(msg maelstrom.Message) error {
		// Unmarshal the body into a struct
		var inputBody struct {
			Message   int64 `json:"message"`
			Timestamp int64 `json:"timestamp"`
		}
		if err := json.Unmarshal(msg.Body, &inputBody); err != nil {
			return err
		}

		// If the timestamp is 0, we need to generate a new one
		if inputBody.Timestamp == 0 {
			inputBody.Timestamp = time.Now().UnixNano()
		}

		// If the value was inserted, we need to propagate it
		if inserted := counter.Insert(inputBody.Message, inputBody.Timestamp); inserted {
			// Create new body for the message
			propagateBody := PropagateBody{
				Type:      "broadcast",
				Message:   inputBody.Message,
				Timestamp: inputBody.Timestamp,
			}

			// Propagate the message to all nodes
			wg := new(sync.WaitGroup)

			for _, node := range topology {
				wg.Add(1)

				go func(
					n *maelstrom.Node,
					wg *sync.WaitGroup,
					node string,
					body PropagateBody,
				) {
					defer wg.Done()

					// Create a new context
					ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
					defer cancel()

					// Send the message to the node
					n.SyncRPC(ctx, node, body)

				}(n, wg, node, propagateBody)
			}

			wg.Wait()
		}

		// Update the message body to return back
		newBody := make(map[string]interface{})
		newBody["type"] = "broadcast_ok"

		// Reply the original message back with the updated body
		return n.Reply(msg, newBody)
	})

	n.Handle("read", func(msg maelstrom.Message) error {
		// Update the message body to return back
		newBody := make(map[string]interface{})
		newBody["type"] = "read_ok"
		newBody["messages"] = counter.Read()

		// Reply the original message back with the updated body
		return n.Reply(msg, newBody)
	})

	n.Handle("topology", func(msg maelstrom.Message) error {
		// Unmarshal the body into a struct
		var inputBody struct {
			Topology map[string][]string `json:"topology"`
		}
		if err := json.Unmarshal(msg.Body, &inputBody); err != nil {
			return err
		}

		// Set the topology
		for _, node := range inputBody.Topology[n.ID()] {
			topology = append(topology, node)
		}

		// Update the message body to return back
		newBody := make(map[string]interface{})
		newBody["type"] = "topology_ok"

		// Reply the original message back with the updated body
		return n.Reply(msg, newBody)
	})

	// Run the node
	if err := n.Run(); err != nil {
		log.Fatal(err)
	}
}
