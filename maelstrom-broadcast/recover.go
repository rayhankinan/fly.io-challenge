package main

const (
	MAXIMUM_MESSAGE_QUEUE_SIZE = 65536
	WORKER_SIZE                = 256
)

type MessageQueueData struct {
	node string
	body PropagateBody
}

type MessageChannel chan MessageQueueData
