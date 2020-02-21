package mqtt

import (
	"fmt"
	"log"
	"os"

	"github.com/joncrlsn/dque"
	"github.com/pkg/errors"
)

// Message is the primative that we store in the queue
type Message struct {
	Topic   string
	Payload []byte
}

// MessageBuilder creates a new Message and returns its pointer
// this is used by the queue to load data from disk
func MessageBuilder() interface{} {
	return &Message{}
}

// MessageQueue holds a list and an element count
type MessageQueue struct {
	messages *dque.DQue
}

func makeDirectory(directory string) error {
	_, err := os.Stat(directory)
	if os.IsNotExist(err) {
		err := os.Mkdir(directory, os.ModePerm)
		if err != nil {
			return errors.Wrap(err, "Mkdir error")
		}
	}
	return nil
}

// NewMessageQueue creates a new empty queue
func NewMessageQueue(name string) (*MessageQueue, error) {
	directory := fmt.Sprintf("%s/messageQueue", os.Getenv("HOME"))
	err := makeDirectory(directory)
	if err != nil {
		log.Fatalf("makeDirectory error %v", err)
	}

	segmentSize := 50
	q, err := dque.NewOrOpen(name, directory, segmentSize, MessageBuilder)
	if err != nil {
		return nil, errors.Wrap(err, "NewOrOpen error")
	}

	return &MessageQueue{
		messages: q,
	}, nil
}

// QueueSize returns the number of queued messages
func (m *MessageQueue) QueueSize() int {
	return m.messages.Size()
}

// QueueMessage adds a message to the queue
func (m *MessageQueue) QueueMessage(msg Message) error {
	err := m.messages.Enqueue(msg)
	if err != nil {
		return errors.Wrap(err, "Enqueue error")
	}
	return nil
}

// FirstMessage gets the next message in the queue
func (m *MessageQueue) FirstMessage() (*Message, bool, error) {
	if m.messages.Size() > 0 {
		mp, err := m.messages.Peek()
		if err != nil {
			return nil, false, errors.Wrap(err, "Peek error")
		}
		_, ok := mp.(*Message)
		if ok {
			return mp.(*Message), true, nil
		}

		msg, ok := mp.(Message)
		if ok {
			return &msg, true, nil
		}
	}
	return nil, false, nil
}

// RemoveFirstMessage removes the message on the front of the list
func (m *MessageQueue) RemoveFirstMessage() error {
	if m.messages.Size() > 0 {
		_, err := m.messages.Dequeue()
		if err != nil {
			return errors.Wrap(err, "Dequeue error")
		}
	}
	return nil
}
