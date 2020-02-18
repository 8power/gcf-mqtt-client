package mqttclient

import (
	"container/list"
)

// Message is the primative that we store in the queue
type Message struct {
	Topic   string
	Payload []byte
}

// MessageQueue holds a list and an element count
type MessageQueue struct {
	messages *list.List
}

// NewMessageQueue creates a new empty queue
func NewMessageQueue() *MessageQueue {
	return &MessageQueue{
		messages: list.New(),
	}
}

// QueueSize returns the number of queued messages
func (m *MessageQueue) QueueSize() int {
	return m.messages.Len()
}

// QueueMessage adds a message to the queue
func (m *MessageQueue) QueueMessage(msg Message) {
	m.messages.PushBack(msg)
}

// FirstMessage gets the next message in the queue
func (m *MessageQueue) FirstMessage() (Message, bool) {
	msg := Message{}
	if m.messages.Len() > 0 {
		element := m.messages.Front()
		msg = element.Value.(Message)
		return msg, true
	}
	return msg, false
}

// RemoveFirstMessage removes the message on the front of the list
func (m *MessageQueue) RemoveFirstMessage() {
	if m.messages.Len() > 0 {
		element := m.messages.Front()
		m.messages.Remove(element)
	}
}
