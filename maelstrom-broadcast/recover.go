package main

const (
	MAXIMUM_MESSAGE_QUEUE_SIZE = 100
	WORKER_SIZE                = 5
)

type MessageQueueData struct {
	node string
	body PropagateBody
}

type MessageChannel chan MessageQueueData
