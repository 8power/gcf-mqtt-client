package mqttclient

import (
	"fmt"
	"log"
	"testing"
	"time"

	"github.com/RobHumphris/veh-structs/vehdata"
	"github.com/golang/protobuf/proto"

	MQTT "github.com/eclipse/paho.mqtt.golang"
)

const (
	Host              = "mqtt.googleapis.com"
	Port              = "8883"
	RootCertFile      = "/home/rob/.certs/roots.pem"
	PrivateKeyPEMFile = "/home/rob/.certs/iot_rsa_private.pem"
	ProjectID         = "vibration-energy-harvesting"
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
		Temperature:    23.25,
		BatteryLevel:   1234,
		CoulombCounter: 1,
		Current:        2,
	}
	return ev
}

func testHander(client MQTT.Client, msg MQTT.Message) {
	fmt.Printf("[handler] Topic: %v\n", msg.Topic())
	fmt.Printf("[handler] Payload: %v\n", msg.Payload())
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

	mc, err := NewMQTTClient(cfg, testHander)
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

	mc, err := NewMQTTClient(cfg, testHander)
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
		fmt.Println("Finito")
	}

	publishMessages(obj, t, mc, 10, 10)
}

func publishMessages(obj *vehdata.VehEvent, t *testing.T, mc *MQTTClient, start int, number int) {
	for i := start; i < start+number; i++ {
		log.Printf("[main] Publishing Message #%d", i)

		obj.Header.SequenceNumber = uint32(i)
		payload, err := proto.Marshal(obj)
		if err != nil {
			t.Errorf("Error marshalling Protobuffer %v", err)
			return
		}

		err = mc.PublishTelemetryEvent(payload)
		if err != nil {
			t.Errorf("Error Publishing payload %v", err)
			return
		}

		time.Sleep(1 * time.Second)
	}
}
