package mqttclient

import (
	"context"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"testing"
	"time"

	jwtGo "github.com/dgrijalva/jwt-go"
	"github.com/pkg/errors"

	MQTT "github.com/eclipse/paho.mqtt.golang"
)

type TestMessage struct {
	Mac            string `json:"mac"`
	Timestamp      int    `json:"timestamp"`
	SequenceNumber int    `json:"sequenceNumber"`
}

const (
	Host        = "mqtt.googleapis.com"
	Port        = "8883"
	ProjectID   = "skillful-mason-244208"
	CloudRegion = "europe-west1"
	RegistryID  = "vibration-energy-harvesting-registry"
)

var (
	RootCertFile      = "/home/" + os.Getenv("USER") + "/.certs/roots.pem"
	PrivateKeyPEMFile = "/home/" + os.Getenv("USER") + "/.certs/iot_rsa_private.pem"
	DeviceID          = "pc-" + os.Getenv("USER") + "-desktop"
)

func testHander(client MQTT.Client, msg MQTT.Message) {
	fmt.Printf("[handler] Topic: %v\n", msg.Topic())
	fmt.Printf("[handler] Payload: %v\n", msg.Payload())
}

// CreateJWT uses the projectId and privateKeyFile to make a Java Web Token (JWT)
func createJWT(projectID string, privateKeyFile string) (string, error) {
	keyBytes, err := ioutil.ReadFile(privateKeyFile)
	if err != nil {
		return "", errors.Wrap(err, "CreateJWT error")
	}

	privateKey, err := jwtGo.ParseRSAPrivateKeyFromPEM(keyBytes)
	if err != nil {
		return "", errors.Wrap(err, "Could not parse private key")
	}

	ts := time.Now().Unix()
	claims := &jwtGo.StandardClaims{
		IssuedAt:  ts,
		ExpiresAt: ts + 20,
		Audience:  projectID,
	}

	token := jwtGo.NewWithClaims(jwtGo.SigningMethodRS256, claims)
	return token.SignedString(privateKey)
}

func credentialsProvider() (username string, password string) {
	fmt.Println("[credentialsProvider] creating new Credentials")
	username = "unused"
	password, err := createJWT(ProjectID, PrivateKeyPEMFile)
	if err != nil {
		fmt.Printf("[credentialsProvider] Error creating JWT %v\n", err)
	}
	return username, password
}

type CommandMessage struct {
	Command   string   `json:"command"`
	Arguments []string `json:"args,omitempty"`
}

func NewCommandMessage(msg []byte) (CommandMessage, error) {
	cmd := CommandMessage{}
	err := json.Unmarshal(msg, &cmd)
	if err != nil {
		return cmd, errors.Wrap(err, "Unmarshal error")
	}
	return cmd, nil
}

func TestCommandMessage(t *testing.T) {
	cmd := CommandMessage{
		Command: "reboot",
	}

	js, err := json.Marshal(&cmd)
	if err != nil {
		t.Fatalf("json Marshal error %v", err)
	}
	fmt.Printf("Command: %s\n", js)
}

func GetClientConfig() ClientConfig {
	return ClientConfig{
		Host:              Host,
		Port:              Port,
		RootCertFile:      RootCertFile,
		PrivateKeyPEMFile: PrivateKeyPEMFile,
		ProjectID:         ProjectID,
		CloudRegion:       CloudRegion,
		RegistryID:        RegistryID,
		DeviceID:          DeviceID,
	}
}

func TestTelemetryClient(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	spec := NewMQTTClientConfig{
		Context:              ctx,
		ClientConfig:         GetClientConfig(),
		DefaultMessageHander: testHander,
		CredentialsProvider:  credentialsProvider,
		OnConnectFunc: func(c *MQTTClient) error {
			err := c.RegisterConfigHandler(func(client MQTT.Client, msg MQTT.Message) {
				fmt.Printf("[config handler] Topic: %v\n", msg.Topic())
				fmt.Printf("[config handler] Payload: %s\n", msg.Payload())
			})
			if err != nil {
				return errors.Wrap(err, "RegisterConfigHandler error")
			}

			err = c.RegisterCommandHandler(func(client MQTT.Client, msg MQTT.Message) {
				fmt.Printf("[command handler] Topic: %v\n", msg.Topic())
				cmd, err := NewCommandMessage(msg.Payload())
				if err != nil {
					fmt.Printf("NewCommandMessage error %v\n", err)
					return
				}
				fmt.Printf("[command handler] CommandMessage: %v\n", cmd)
			})
			if err != nil {
				return errors.Wrap(err, "RegisterCommandHandler error")
			}
			return nil
		},
	}

	mc, err := NewMQTTClient(spec)
	if err != nil {
		t.Errorf("Error raised in NewMQTTClient: %v\n", err)
	}

	fmt.Printf("IsConnectionGood %t\n", mc.IsConnectionGood())

	err = mc.Connect()
	if err != nil {
		t.Errorf("Error raised in Connect: %v\n", err)
	}

	fmt.Printf("IsConnectionGood %t\n", mc.IsConnectionGood())

	fmt.Println("Publishing a config type of thing")
	publishConfig := `{"config": "test here, there, there, and everywhere"}`
	mc.PublishState([]byte(publishConfig))

	loop := true

	fmt.Printf("IsConnectionGood %t\n", mc.IsConnectionGood())

	go func() {
		for loop {
			fmt.Printf("IsConnectionGood %t\n", mc.IsConnectionGood())
			if !mc.IsConnectionGood() {
				fmt.Printf("Its fallen off a cliff\n")
			}
			time.Sleep(100 * time.Millisecond)
		}
	}()

	var waitTime = 10 * time.Second
	fmt.Printf("Now waiting for %f seconds, just to see what happens\n\n", waitTime.Seconds())
	select {
	case <-time.After(waitTime):
		fmt.Println("Finito")
	}

	loop = false

	err = mc.RemoveCommandHandler()
	if err != nil {
		t.Errorf("RemoveCommandHandler Error: %v\n", err)
	}

	err = mc.RemoveConfigHandler()
	if err != nil {
		t.Errorf("RemoveConfigHandler Error: %v\n", err)
	}

	err = mc.Disconnect()
	if err != nil {
		t.Errorf("Error raised during Disconnect: %v\n", err)
	}
}

func TestClient(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	spec := NewMQTTClientConfig{
		Context:              ctx,
		ClientConfig:         GetClientConfig(),
		DefaultMessageHander: testHander,
		CredentialsProvider:  credentialsProvider,
		OnConnectFunc: func(c *MQTTClient) error {
			err := c.RegisterConfigHandler(func(client MQTT.Client, msg MQTT.Message) {
				fmt.Printf("[config handler] Topic: %v\n", msg.Topic())
				fmt.Printf("[config handler] Payload: %s\n", msg.Payload())
			})
			if err != nil {
				return errors.Wrap(err, "RegisterConfigHandler error")
			}

			err = c.RegisterCommandHandler(func(client MQTT.Client, msg MQTT.Message) {
				fmt.Printf("[command handler] Topic: %v\n", msg.Topic())
				fmt.Printf("[command handler] Payload: %v\n", msg.Payload())
			})
			if err != nil {
				return errors.Wrap(err, "RegisterCommandHandler error")
			}
			return nil
		},
	}

	mc, err := NewMQTTClient(spec)
	if err != nil {
		t.Errorf("Error raised in NewMQTTClient: %v\n", err)
		return
	}

	err = mc.Connect()

	if err != nil {
		t.Errorf("Error raised in connecting: %v\n", err)
		return
	}

	obj := &TestMessage{
		Mac:            "AA:BB:CC:DD:EE:FF",
		SequenceNumber: 1,
		Timestamp:      int(time.Now().Unix()),
	}
	publishMessages(obj, t, mc, 0, 0)

	delay := time.Duration(5)
	fmt.Printf("Now waiting for %d minutes for JWT to expire, with %d second messages\n", delay, delay)
	ctx2, cancel2 := context.WithTimeout(context.Background(), delay*time.Minute)

	byebye := make(chan bool)

	go func() {
		i := 1
		for {
			select {
			case <-time.After(delay * time.Second):
				publishMessages(obj, t, mc, i, i)
				i++
				continue
			case <-ctx2.Done():
				cancel2()
				fmt.Println("halted operation2")
				byebye <- true
				return
			}
		}
	}()

	select {
	case <-byebye:
		break
	}

	// Now disconnect and ensure that nothing more is sent
	mc.Disconnect()
	time.Sleep(5 * time.Second)

	payload, err := json.Marshal(obj)
	if err != nil {
		t.Errorf("JSON Marshal error %v", err)
	} else {
		mc.PublishTelemetryEvent(payload)
	}
}

func publishMessages(obj *TestMessage, t *testing.T, mc *MQTTClient, start int, number int) {
	for i := start; i < start+number; i++ {
		log.Printf("[main] Publishing Message #%d", i)
		obj.SequenceNumber = i

		payload, err := json.Marshal(obj)
		if err != nil {
			t.Errorf("JSON Marshal error %v", err)
		} else {
			err = mc.PublishTelemetryEvent(payload)
			if err != nil {
				t.Errorf("PublishTelemetryEvent error %v", err)
			}
			time.Sleep(1 * time.Second)
		}
	}
}
