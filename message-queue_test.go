package mqttclient

import (
	"fmt"
	"os"
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

func TestMakeDirectory(t *testing.T) {
	newDir := fmt.Sprintf("%s/queue-test-%d", os.Getenv("HOME"), time.Now().UnixNano())

	err := makeDirectory(newDir)
	if err != nil {
		t.Errorf("Error should be nil, not %v", err)
	}

	err = makeDirectory(newDir)
	if err != nil {
		t.Errorf("Error should be nil as we ignore already existing errors, not %v", err)
	}

	err = os.RemoveAll(newDir)
	if err != nil {
		t.Fatalf("RemoveAll error %v", err)
	}
}

func TestMessageQueue(t *testing.T) {
	mq, err := NewMessageQueue("test-queue")
	if err != nil {
		t.Fatalf("NewMessageQueue error %v", err)
	}

	for i := 0; i < 10; i++ {
		mq.QueueMessage(makeMessage())
	}

	for i := 0; i < 11; i++ {
		dst, ok, err := mq.FirstMessage()
		if ok {
			fmt.Printf("Message: %s\n", dst.Topic)
		} else {
			if err != nil {
				t.Errorf("FirstMessage error %v", err)
				break
			}
			fmt.Printf("Message ok? %t\n", ok)
		}
		mq.RemoveFirstMessage()
	}
}
