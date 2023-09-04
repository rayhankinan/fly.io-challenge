package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"sync"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

type Counter struct {
	data          []float64
	readWriteLock sync.RWMutex
}

func (c *Counter) Read() []float64 {
	c.readWriteLock.RLock()
	defer c.readWriteLock.RUnlock()

	return c.data
}

func (c *Counter) Append(value float64) {
	c.readWriteLock.Lock()
	defer c.readWriteLock.Unlock()

	c.data = append(c.data, value)
}

func main() {
	// Create a new node
	n := maelstrom.NewNode()

	// Create a counter to store the values
	counter := &Counter{
		data:          []float64{},
		readWriteLock: sync.RWMutex{},
	}

	n.Handle("broadcast", func(msg maelstrom.Message) error {
		// Unmarshal the body into a loosely typed map
		var inputBody map[string]interface{}
		if err := json.Unmarshal(msg.Body, &inputBody); err != nil {
			return err
		}

		value, ok := inputBody["message"].(float64)
		if !ok {
			return errors.New(fmt.Sprintf("Invalid message type: %T", inputBody["message"]))
		}

		// Add the value to the counter with a lock
		counter.Append(value)

		// Update the message body to return back
		newBody := make(map[string]interface{})

		newBody["type"] = "broadcast_ok"

		// Reply the original message back with the updated body
		return n.Reply(msg, newBody)
	})

	n.Handle("read", func(msg maelstrom.Message) error {
		// Unmarshal the body into a loosely typed map
		var inputBody map[string]interface{}
		if err := json.Unmarshal(msg.Body, &inputBody); err != nil {
			return err
		}

		// Update the message body to return back
		newBody := make(map[string]interface{})

		newBody["type"] = "read_ok"
		newBody["messages"] = counter.Read()

		// Reply the original message back with the updated body
		return n.Reply(msg, newBody)
	})

	n.Handle("topology", func(msg maelstrom.Message) error {
		// Unmarshal the body into a loosely typed map
		var inputBody map[string]interface{}
		if err := json.Unmarshal(msg.Body, &inputBody); err != nil {
			return err
		}

		// Update the message body to return back
		newBody := make(map[string]interface{})

		newBody["type"] = "topology_ok"

		// Reply the original message back with the updated body
		return n.Reply(msg, newBody)
	})

	if err := n.Run(); err != nil {
		log.Fatal(err)
	}
}
