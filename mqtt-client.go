package mqttclient

import (
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"io/ioutil"
	"log"
	"time"

	"github.com/gogo/protobuf/proto"
	"github.com/pkg/errors"

	"github.com/RobHumphris/veh-structs/vehdata"

	MQTT "github.com/eclipse/paho.mqtt.golang"
)

// Qos is the default MQTT Quality of Service
const Qos = 0

// MQTTClientConfig holds the essential elements needed to configure the MQTTClient
type MQTTClientConfig struct {
	Host              string
	Port              string
	RootCertFile      string
	PrivateKeyPEMFile string
	ProjectID         string
	CloudRegion       string
	RegistryID        string
	DeviceID          string
}

// MQTTTopics hold the two Topics that the GCF cloud uses
type MQTTTopics struct {
	Config    string
	Telemetry string
	State     string
}

// MQTTClient contains the essential elements that the MQTT client needs to function
type MQTTClient struct {
	Client    MQTT.Client
	Values    *MQTTClientConfig
	Topics    *MQTTTopics
	connected bool
}

func connectionLostHandler(client MQTT.Client, err error) {
	fmt.Printf("[connectionLostHandler] invoked with error %v\n", err)
}

// NewMQTTClient intialises and returns a new instance of a MQTTClient using
// the passed MQTTClientValues and the default MessageHandler
func NewMQTTClient(cfg *MQTTClientConfig, defaultHandler MQTT.MessageHandler, credentialsProvider MQTT.CredentialsProvider) (mc *MQTTClient, err error) {

	certpool := x509.NewCertPool()
	pemCerts, err := ioutil.ReadFile(cfg.RootCertFile)
	if err != nil {
		return nil, errors.Wrap(err, "Failed to read Root Cert File")
	}

	certpool.AppendCertsFromPEM(pemCerts)

	config := &tls.Config{
		RootCAs:            certpool,
		ClientAuth:         tls.NoClientCert,
		ClientCAs:          nil,
		InsecureSkipVerify: true,
		Certificates:       []tls.Certificate{},
		MinVersion:         tls.VersionTLS12,
	}

	opts := MQTT.NewClientOptions()
	opts.AddBroker(getMQTTBrokerAddress(cfg))
	opts.SetClientID(getMQTTClientID(cfg))
	opts.SetTLSConfig(config)
	opts.SetAutoReconnect(true)
	opts.SetConnectionLostHandler(connectionLostHandler)
	opts.SetCredentialsProvider(credentialsProvider)
	opts.SetDefaultPublishHandler(defaultHandler)

	mc = &MQTTClient{
		Client:    MQTT.NewClient(opts),
		Values:    cfg,
		Topics:    getMQTTTopics(cfg),
		connected: false,
	}

	return mc, nil
}

// Connect attempts to connect to the MQTT Broker and returns an error if
// unsuccessful
func (mc *MQTTClient) Connect() error {
	token := mc.Client.Connect()
	okflag := token.WaitTimeout(6 * time.Second)
	if !okflag {
		return fmt.Errorf("Timeout waiting to connect to")
	}
	if token.Error() != nil {
		return errors.Wrap(token.Error(), "Error connecting to MQTT Broker")
	}
	log.Println("[Connect] MQTT Client connected")
	mc.connected = true
	return nil
}

// Disconnect from the MQTT client
func (mc *MQTTClient) Disconnect() {
	mc.Client.Disconnect(250)
	mc.connected = false
}

// NotConnected is raised when...
var NotConnected = fmt.Errorf("Not Connected")

// RegisterConfigHandler subscribes to the Config topic, and assigns the
// passed handler function to it.
func (mc *MQTTClient) RegisterConfigHandler(handler MQTT.MessageHandler) error {
	if mc == nil || !mc.connected {
		return NotConnected
	}

	mc.Client.Subscribe(mc.Topics.Config, Qos, handler)
	return nil
}

// RegisterStateHandler subscribes the passed handler to the State topic.
func (mc *MQTTClient) RegisterStateHandler(handler MQTT.MessageHandler) error {
	if mc == nil || !mc.connected {
		return NotConnected
	}

	mc.Client.Subscribe(mc.Topics.State, Qos, handler)
	return nil
}

// RegisterTelemetryHandler subscribes to the Telemetry topic, and assigns the
// passed handler function to it.
func (mc *MQTTClient) RegisterTelemetryHandler(handler MQTT.MessageHandler) error {
	if mc == nil || !mc.connected {
		return NotConnected
	}

	mc.Client.Subscribe(mc.Topics.Telemetry, Qos, handler)
	return nil
}

// PublishTelemetryEvent sends the payload to the MQTT broker, if it doesnt
// work we get an error.
func (mc *MQTTClient) PublishTelemetryEvent(messages []vehdata.VehMessage) error {
	if mc == nil || !mc.connected {
		return NotConnected
	}

	for _, msg := range messages {
		payload, err := proto.Marshal(&msg)
		if err != nil {
			return errors.Wrap(err, "proto marshal error")
		}
		err = mc.publish(payload)
		if err != nil {
			return errors.Wrap(err, "publish error")
		}
	}
	return nil
}

func (mc *MQTTClient) publish(payload []byte) error {
	token := mc.Client.Publish(mc.Topics.Telemetry, Qos, false, payload)
	okflag := token.WaitTimeout(5 * time.Second)
	if !okflag {
		return fmt.Errorf("Timeout publishing telemetry event")
	}
	if token.Error() != nil {
		return errors.Wrap(token.Error(), "Error publishing telemetry event")
	}
	return nil
}

func getMQTTClientID(cfg *MQTTClientConfig) string {
	return fmt.Sprintf("projects/%s/locations/%s/registries/%s/devices/%s",
		cfg.ProjectID,
		cfg.CloudRegion,
		cfg.RegistryID,
		cfg.DeviceID)
}

func getMQTTTopics(cfg *MQTTClientConfig) (topics *MQTTTopics) {
	return &MQTTTopics{
		Config:    fmt.Sprintf("/devices/%s/config", cfg.DeviceID),
		Telemetry: fmt.Sprintf("/devices/%s/events", cfg.DeviceID),
		State:     fmt.Sprintf("/devices/%s/state", cfg.DeviceID),
	}
}

func getMQTTBrokerAddress(cfg *MQTTClientConfig) string {
	return fmt.Sprintf("ssl://%s:%s", cfg.Host, cfg.Port)
}
