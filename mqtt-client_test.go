package mqttclient

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
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
	Host              = "mqtt.googleapis.com"
	Port              = "8883"
	RootCertFile      = "/home/rob/.certs/roots.pem"
	PrivateKeyPEMFile = "/home/rob/.certs/iot_rsa_private.pem"
	ProjectID         = "skillful-mason-244208"
	CloudRegion       = "europe-west1"
	RegistryID        = "vibration-energy-harvesting-registry"
	DeviceID          = "dell-development-laptop"
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
		ExpiresAt: ts + 1200,
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

func TestTelemetryClient(t *testing.T) {
	cfg := &MQTTClientConfig{
		Host:              Host,
		Port:              Port,
		RootCertFile:      RootCertFile,
		PrivateKeyPEMFile: PrivateKeyPEMFile,
		ProjectID:         ProjectID,
		CloudRegion:       CloudRegion,
		RegistryID:        RegistryID,
		DeviceID:          DeviceID,
	}

	mc, err := NewMQTTClient(cfg, testHander, credentialsProvider)
	if err != nil {
		t.Errorf("Error raised in NewMQTTClient: %v\n", err)
	}

	err = mc.Connect()
	if err != nil {
		t.Errorf("Error raised in Connect: %v\n", err)
	}

	isConnected := mc.IsConnected()
	fmt.Printf("IsConnected returns %t\n", isConnected)

	err = mc.RegisterConfigHandler(func(client MQTT.Client, msg MQTT.Message) {
		fmt.Printf("[config handler] Topic: %v\n", msg.Topic())
		fmt.Printf("[config handler] Payload: %s\n", msg.Payload())
	})
	if err != nil {
		t.Errorf("Error raised in RegisterConfigHandler: %v\n", err)
	}

	err = mc.RegisterCommandHandler(func(client MQTT.Client, msg MQTT.Message) {
		fmt.Printf("[command handler] Topic: %v\n", msg.Topic())
		fmt.Printf("[command handler] Payload: %v\n", msg.Payload())
	})
	if err != nil {
		t.Errorf("Error raised in RegisterCommandHandler: %v\n", err)
	}

	fmt.Println("Publishing a config type of thing")
	publishConfig := `{"config": "test here, there, there, and everywhere"}`
	err = mc.PublishState([]byte(publishConfig))
	if err != nil {
		t.Errorf("Error raised in PublishConfig: %v\n", err)
	}

	var waitTime = 30
	fmt.Printf("Now waiting for %d seconds, just to see what happens\n", waitTime)
	select {
	case <-time.After(30 * time.Second):
		fmt.Println("Finito")
	}

	err = mc.Disconnect()
	if err != nil {
		t.Errorf("Error raised during Disconnect: %v\n", err)
	}
}

func TestClient(t *testing.T) {
	cfg := &MQTTClientConfig{
		Host:              Host,
		Port:              Port,
		RootCertFile:      RootCertFile,
		PrivateKeyPEMFile: PrivateKeyPEMFile,
		ProjectID:         ProjectID,
		CloudRegion:       CloudRegion,
		RegistryID:        RegistryID,
		DeviceID:          DeviceID,
	}

	mc, err := NewMQTTClient(cfg, testHander, credentialsProvider)
	if err != nil {
		t.Errorf("Error raised in NewMQTTClient: %v\n", err)
		return
	}

	err = mc.Connect()
	if err != nil {
		t.Errorf("Error raised in connecting: %v\n", err)
		return
	}

	err = mc.RegisterConfigHandler(func(client MQTT.Client, msg MQTT.Message) {
		fmt.Printf("[config handler] Topic: %v\n", msg.Topic())
		fmt.Printf("[config handler] Payload: %s\n", msg.Payload())

		reply := fmt.Sprintf("%s reply", msg.Payload())
		mc.PublishState([]byte(reply))
	})
	if err != nil {
		t.Errorf("Error raised in RegisterConfigHandler: %v\n", err)
	}

	err = mc.RegisterCommandHandler(func(client MQTT.Client, msg MQTT.Message) {
		fmt.Printf("[command handler] Topic: %v\n", msg.Topic())
		fmt.Printf("[command handler] Payload: %v\n", msg.Payload())
	})
	if err != nil {
		t.Errorf("Error raised in RegisterCommandHandler: %v\n", err)
	}

	obj := &TestMessage{
		Mac:            "AA:BB:CC:DD:EE:FF",
		SequenceNumber: 1,
		Timestamp:      int(time.Now().Unix()),
	}
	publishMessages(obj, t, mc, 0, 10)

	fmt.Println("Now waiting for 30 seconds for JWT to expire")
	select {
	case <-time.After(30 * time.Second):
		fmt.Println("Times up, should have needed a new JWT by now")
	}

	publishMessages(obj, t, mc, 10, 10)

	// Now disconnect and ensure that nothing more is sent
	mc.Disconnect()
	time.Sleep(5 * time.Second)

	payload, err := json.Marshal(obj)
	if err != nil {
		t.Errorf("JSON Marshal error %v", err)
	} else {
		err = mc.PublishTelemetryEvent(payload)
		if err == nil {
			t.Errorf("should not allow publishing of messages")
		}

		if err != ErrorNotConnected {
			t.Errorf("error should be NotConnected not %v", err)
		}
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
				t.Errorf("Error Publishing payload %v", err)
				return
			}
			time.Sleep(1 * time.Second)
		}
	}
}
