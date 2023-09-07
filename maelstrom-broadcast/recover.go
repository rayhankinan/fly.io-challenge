package main

type MessageQueueData struct {
	node string
	body PropagateBody
}

type MessageChannel chan MessageQueueData
