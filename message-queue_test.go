package mqttclient

import (
	"fmt"
	"testing"
	"time"
)

func makeMessage() Message {
	now := time.Now().UnixNano()
	return Message{
		Topic:   fmt.Sprintf("Topic-%d", now),
		Payload: []byte(fmt.Sprintf("Payload:%d%d%d", now, now, now)),
	}
}

func TestMessageQueue(t *testing.T) {
	mq := NewMessageQueue()
	for i := 0; i < 10; i++ {
		mq.QueueMessage(makeMessage())
	}

	for i := 0; i < 11; i++ {
		dst, ok := mq.FirstMessage()
		if ok {
			fmt.Printf("Message: %s\n", dst.Topic)
		} else {
			fmt.Printf("Message ok? %t\n", ok)
		}
		mq.RemoveFirstMessage()
	}
}
