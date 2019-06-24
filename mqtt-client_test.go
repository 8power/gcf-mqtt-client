package mqttclient

import (
	"fmt"
	"io/ioutil"
	"log"
	"testing"
	"time"

	"github.com/RobHumphris/veh-structs/vehdata"
	"github.com/RobHumphris/veh-structs/vehpubsub"
	jwtGo "github.com/dgrijalva/jwt-go"
	"github.com/golang/protobuf/proto"
	"github.com/pkg/errors"

	MQTT "github.com/eclipse/paho.mqtt.golang"
)

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

func f() *vehdata.VehEvent {
	ev := &vehdata.VehEvent{}
	ev.Header = &vehdata.VehEventHeader{
		Timestamp:      uint32(time.Now().Unix()),
		SequenceNumber: 0,
		Event:          4,
		Length:         18,
	}

	ev.SensorEvent = &vehdata.VehSensorEvent{
		Temperature:  23.25,
		BatteryLevel: 1234,
		Counter:      1,
		Current:      2,
	}
	return ev
}

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
	fmt.Printf("[credentialsProvider] creating new Credentials")
	username = "unused"
	password, err := createJWT(ProjectID, PrivateKeyPEMFile)
	if err != nil {
		fmt.Printf("[credentialsProvider] Error creating JWT %v", err)
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
		return
	}

	err = mc.Connect()
	if err != nil {
		t.Errorf("Error raised in connecting: %v\n", err)
		return
	}

	mc.RegisterTelemetryHandler(func(client MQTT.Client, msg MQTT.Message) {
		fmt.Printf("[Telemetry handler] Topic: %v\n", msg.Topic())
		fmt.Printf("[Telemetry handler] Payload: %v\n", msg.Payload())
	})

	mc.RegisterStateHandler(func(client MQTT.Client, msg MQTT.Message) {
		fmt.Printf("[State handler] Topic: %v\n", msg.Topic())
		fmt.Printf("[State handler] Payload: %v\n", msg.Payload())
	})

	mc.RegisterConfigHandler(func(client MQTT.Client, msg MQTT.Message) {
		fmt.Printf("[config handler] Topic: %v\n", msg.Topic())
		fmt.Printf("[config handler] Payload: %v\n", msg.Payload())
	})

	fmt.Println("Now waiting for 5 minutes, just to see what happens")
	select {
	case <-time.After(5 * time.Minute):
		fmt.Println("Finito")
		return
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

	mc.RegisterConfigHandler(func(client MQTT.Client, msg MQTT.Message) {
		fmt.Printf("[config handler] Topic: %v\n", msg.Topic())
		fmt.Printf("[config handler] Payload: %v\n", msg.Payload())
	})

	obj := f()
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
	msg, err := createMessage(obj)
	err = mc.PublishTelemetryEvent(msg)
	if err == nil {
		t.Errorf("should not allow publishing of messages")
	}

	if err != NotConnected {
		t.Errorf("error should be NotConnected not %v", err)
	}
}

func createMessage(obj *vehdata.VehEvent) ([]vehdata.VehMessage, error) {
	evs := &vehdata.VehEvents{}
	evs.DeviceMACAddress = "00:01:02:03:04:05"
	evs.Events = append(evs.Events, obj)
	payload, err := proto.Marshal(evs)
	if err != nil {
		return nil, fmt.Errorf("Error marshalling Protobuffer %v", err)
	}

	return vehpubsub.PackVehMessages(payload, vehdata.VehMessage_VehGatewayStatus)
}

func publishMessages(obj *vehdata.VehEvent, t *testing.T, mc *MQTTClient, start int, number int) {
	for i := start; i < start+number; i++ {
		log.Printf("[main] Publishing Message #%d", i)

		obj.Header.SequenceNumber = uint32(i)
		messages, err := createMessage(obj)
		if err != nil {
			t.Errorf("PackVehMessages error %v", err)
			return
		}

		err = mc.PublishTelemetryEvent(messages)
		if err != nil {
			t.Errorf("Error Publishing payload %v", err)
			return
		}
		time.Sleep(1 * time.Second)
	}
}
