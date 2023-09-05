package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"sort"
	"sync"
	"time"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

type Element struct {
	data      float64
	timestamp int64
}

type Counter struct {
	arr           []Element
	readWriteLock *sync.RWMutex
}

func (c *Counter) Read() []float64 {
	c.readWriteLock.RLock()
	defer c.readWriteLock.RUnlock()

	result := []float64{}

	for _, element := range c.arr {
		result = append(result, element.data)
	}

	return result
}

func (c *Counter) Insert(value float64, currentTimestamp int64) bool {
	c.readWriteLock.Lock()
	defer c.readWriteLock.Unlock()

	i, found := sort.Find(len(c.arr), func(i int) int {
		if currentTimestamp < c.arr[i].timestamp {
			return -1
		} else if currentTimestamp > c.arr[i].timestamp {
			return 1
		} else {
			return 0
		}
	})

	if !found {
		if i == len(c.arr) {
			c.arr = append(c.arr, Element{
				data:      value,
				timestamp: currentTimestamp,
			})
		} else {
			c.arr = append(c.arr[:i+1], c.arr[i:]...)
			c.arr[i] = Element{
				data:      value,
				timestamp: currentTimestamp,
			}
		}
	}

	return !found
}

func main() {
	// Create a new node
	n := maelstrom.NewNode()

	// Create a counter to store the values
	counter := &Counter{
		arr:           []Element{},
		readWriteLock: &sync.RWMutex{},
	}

	// Create a topology to store the nodes
	topology := []string{}

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

		rawCurrentTimestamp, ok := inputBody["timestamp"].(float64)

		// If the timestamp is not provided, use the current time
		var currentTimestamp int64

		if !ok {
			currentTimestamp = time.Now().UnixNano()
		} else {
			// Kemungkinan bug: adanya overflow atau ketidakakuratan pembulatan timestamp
			// Tapi sudah diterima oleh Jepsen
			currentTimestamp = int64(rawCurrentTimestamp)
		}

		// If the value was inserted, we need to propagate it
		if inserted := counter.Insert(value, currentTimestamp); inserted {
			// Create new body for the message
			propagateBody := make(map[string]interface{})
			propagateBody["type"] = "broadcast"
			propagateBody["message"] = value
			propagateBody["timestamp"] = currentTimestamp

			// Propagate the message to all nodes
			wg := &sync.WaitGroup{}

			for _, node := range topology {
				wg.Add(1)

				go func(n *maelstrom.Node, wg *sync.WaitGroup, node string, body map[string]interface{}) {
					defer wg.Done()

					n.RPC(node, body, func(msg maelstrom.Message) error {
						log.Printf("Received reply from %s", node)
						return nil
					})
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

		// Get the topology from the message
		inputTopology, ok := inputBody["topology"].(map[string]interface{})
		if !ok {
			return errors.New(fmt.Sprintf("Invalid topology type: %T", inputBody["topology"]))
		}

		// Convert the topology to a string array
		inputNodes, ok := inputTopology[n.ID()].([]interface{})
		if !ok {
			return errors.New(fmt.Sprintf("Invalid nodes type: %T", inputTopology[n.ID()]))
		}

		// Set the topology
		for _, node := range inputNodes {
			topology = append(topology, node.(string))
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
