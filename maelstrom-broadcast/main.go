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
	Type      string   `json:"type"`
	Message   int64    `json:"message"`
	Timestamp int64    `json:"timestamp"`
	History   []string `json:"history"`
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

	// Create a channel to store the messages
	messageChannel := make(MessageChannel, MAXIMUM_MESSAGE_QUEUE_SIZE)

	n.Handle("broadcast", func(msg maelstrom.Message) error {
		// Unmarshal the body into a struct
		var inputBody PropagateBody
		if err := json.Unmarshal(msg.Body, &inputBody); err != nil {
			return err
		}

		// If the timestamp is 0, we need to generate a new one
		if inputBody.Timestamp == 0 {
			inputBody.Timestamp = time.Now().UnixNano()
		}

		if inputBody.History == nil {
			inputBody.History = []string{}
		}

		// If the value was inserted, we need to propagate it
		// TODO: Bisa ditambahkan ke dalam buffered channel (asynchronous)
		if inserted := counter.Insert(inputBody.Message, inputBody.Timestamp); inserted {
			// Create new body for the message
			propagateBody := PropagateBody{
				Type:      "broadcast",
				Message:   inputBody.Message,
				Timestamp: inputBody.Timestamp,
				History:   append(inputBody.History, n.ID()),
			}

			// Propagate the message to all nodes
			wg := new(sync.WaitGroup)

			for _, node := range difference(topology, propagateBody.History) {
				wg.Add(1)

				go func(
					n *maelstrom.Node,
					wg *sync.WaitGroup,
					messageChannel MessageChannel,
					node string,
					body PropagateBody,
				) {
					defer wg.Done()

					// Create a new context
					ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
					defer cancel()

					// Send the message to the node
					if _, err := n.SyncRPC(ctx, node, body); err != nil {
						// If the node is down, we need to store the message
						messageChannel <- MessageQueueData{
							node: node,
							body: body,
						}
					}
				}(n, wg, messageChannel, node, propagateBody)
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

	// Run the watcher
	go func(
		n *maelstrom.Node,
		messageChannel MessageChannel,
	) {
		for {
			select {
			// Get the message from the channel
			case message := <-messageChannel:
				// Create a new goroutine to send the message
				go func(
					n *maelstrom.Node,
					messageChannel MessageChannel,
					message MessageQueueData,
				) {
					// Create a new context
					ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
					defer cancel()

					// Send the message to the node
					if _, err := n.SyncRPC(ctx, message.node, message.body); err != nil {
						// If the node is down, we need to store the message
						messageChannel <- MessageQueueData{
							node: message.node,
							body: message.body,
						}
					}
				}(n, messageChannel, message)
			}
		}
	}(
		n,
		messageChannel,
	)

	// Create recover function
	defer func() {
		if err := recover(); err != nil {
			log.Fatal(err)
		}

		// Close the message channel
		close(messageChannel)
	}()

	// Run the node
	if err := n.Run(); err != nil {
		panic(err)
	}
}
