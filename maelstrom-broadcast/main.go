package main

import (
	"encoding/json"
	"log"
	"sort"
	"sync"
	"time"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

type Element struct {
	data      int64
	timestamp int64
}

type Counter struct {
	arr           []Element
	readWriteLock *sync.RWMutex
}

func (c *Counter) Read() []int64 {
	c.readWriteLock.RLock()
	defer c.readWriteLock.RUnlock()

	result := []int64{}

	for _, element := range c.arr {
		result = append(result, element.data)
	}

	return result
}

func (c *Counter) Insert(value int64, currentTimestamp int64) bool {
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
			propagateBody := struct {
				Type      string `json:"type"`
				Message   int64  `json:"message"`
				Timestamp int64  `json:"timestamp"`
			}{
				Type:      "broadcast",
				Message:   inputBody.Message,
				Timestamp: inputBody.Timestamp,
			}

			// Propagate the message to all nodes
			wg := &sync.WaitGroup{}

			for _, node := range topology {
				wg.Add(1)

				go func(n *maelstrom.Node,
					wg *sync.WaitGroup,
					node string,
					body struct {
						Type      string `json:"type"`
						Message   int64  `json:"message"`
						Timestamp int64  `json:"timestamp"`
					}) {
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

	if err := n.Run(); err != nil {
		log.Fatal(err)
	}
}
