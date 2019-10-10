package mqttclient

import (
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"io/ioutil"
	"log"
	"time"

	"github.com/pkg/errors"

	MQTT "github.com/eclipse/paho.mqtt.golang"
)

// Qos is the default MQTT Quality of Service
const Qos = 0

// ErrorNotConnected is raised when...
var ErrorNotConnected = fmt.Errorf("Not Connected")
var clientDefaultTimeout = (5 * time.Second)

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

// MQTTClient contains the essential elements that the MQTT client needs to function
type MQTTClient struct {
	Client                    MQTT.Client
	Values                    *MQTTClientConfig
	connected                 bool
	telemetryPublishTopic     string
	statePublishTopic         string
	configSubscriptionTopic   string
	commandsSubscriptionTopic string
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
		connected: false,
	}

	mc.telemetryPublishTopic = mc.formatMQTTTopicString("events")
	mc.statePublishTopic = mc.formatMQTTTopicString("state")
	mc.configSubscriptionTopic = mc.formatMQTTTopicString("config")
	mc.commandsSubscriptionTopic = mc.formatMQTTTopicString("commands/#")

	return mc, nil
}

// Connect attempts to connect to the MQTT Broker and returns an error if
// unsuccessful
func (mc *MQTTClient) Connect() error {
	log.Println("[MQTTClient] Connecting")
	token := mc.Client.Connect()
	okflag := token.WaitTimeout(clientDefaultTimeout)
	if !okflag {
		return fmt.Errorf("Timeout waiting to connect")
	}
	if token.Error() != nil {
		return errors.Wrap(token.Error(), "Error connecting to MQTT Broker")
	}
	log.Println("[MQTTClient] Connected")
	mc.connected = true
	return nil
}

// Disconnect from the MQTT client
func (mc *MQTTClient) Disconnect() error {
	if mc == nil || !mc.connected {
		return ErrorNotConnected
	}

	log.Println("[MQTTClient] Disconnecting")
	mc.Client.Disconnect(250)
	mc.connected = false
	return nil
}

// IsConnected returns true if connected
func (mc *MQTTClient) IsConnected() bool {
	if mc == nil || !mc.connected {
		return false
	}
	return true
}

func (mc *MQTTClient) registerHandler(topic string, handler MQTT.MessageHandler) error {
	if mc == nil || !mc.connected {
		return ErrorNotConnected
	}
	token := mc.Client.Subscribe(topic, Qos, handler)
	okflag := token.WaitTimeout(clientDefaultTimeout)
	if !okflag {
		return fmt.Errorf("MQTT Subscribe timeout")
	}
	if token.Error() != nil {
		return errors.Wrap(token.Error(), "MQTT Subscribe error")
	}
	return nil
}

// RegisterConfigHandler registers the `handler` to the IOT cloud's device config topic
func (mc *MQTTClient) RegisterConfigHandler(handler MQTT.MessageHandler) error {
	return mc.registerHandler(mc.configSubscriptionTopic, handler)
}

// RegisterCommandHandler registers the `handler` to the IOT cloud's device command topic
func (mc *MQTTClient) RegisterCommandHandler(handler MQTT.MessageHandler) error {
	return mc.registerHandler(mc.commandsSubscriptionTopic, handler)
}

func (mc *MQTTClient) publish(topic string, payload []byte) error {
	if mc == nil || !mc.connected {
		return ErrorNotConnected
	}

	fmt.Printf("Publishing to Topic: %s\n", topic)
	token := mc.Client.Publish(topic, Qos, false, payload)
	okflag := token.WaitTimeout(clientDefaultTimeout)
	if !okflag {
		return fmt.Errorf("MQTT publishing timeout")
	}
	if token.Error() != nil {
		return errors.Wrap(token.Error(), "MQTT publishing error")
	}
	return nil
}

// PublishTelemetryEvent sends the payload to the MQTT broker, if it doesnt
// work we get an error.
func (mc *MQTTClient) PublishTelemetryEvent(payload []byte) error {
	return mc.publish(mc.telemetryPublishTopic, payload)
}

// PublishState sends the payload to the MQTT broker's Config topic.
func (mc *MQTTClient) PublishState(payload []byte) error {
	return mc.publish(mc.statePublishTopic, payload)
}

func getMQTTClientID(cfg *MQTTClientConfig) string {
	return fmt.Sprintf("projects/%s/locations/%s/registries/%s/devices/%s",
		cfg.ProjectID,
		cfg.CloudRegion,
		cfg.RegistryID,
		cfg.DeviceID)
}

func (mc *MQTTClient) formatMQTTTopicString(topic string) string {
	return fmt.Sprintf("/devices/%s/%s", mc.Values.DeviceID, topic)
}

func (mc *MQTTClient) formatMQTTPublishTopicString(topic string) string {
	return fmt.Sprintf("/projects/%s/topics/%s", mc.Values.ProjectID, topic)
}

func getMQTTBrokerAddress(cfg *MQTTClientConfig) string {
	brokerAddress := fmt.Sprintf("ssl://%s:%s", cfg.Host, cfg.Port)
	log.Printf("[MQTTClient] getMQTTBrokerAddress %s", brokerAddress)
	return brokerAddress
}
