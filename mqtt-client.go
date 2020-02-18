package mqttclient

import (
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"io/ioutil"
	"log"
	"time"

	"github.com/pkg/errors"
	"golang.org/x/net/context"

	retry "github.com/avast/retry-go"
	MQTT "github.com/eclipse/paho.mqtt.golang"
)

// Qos is the default MQTT Quality of Service
const Qos = 1

// ErrorNotConnected is raised when...
var ErrorNotConnected = fmt.Errorf("Not Connected")

// ErrorMQTTTimeout returned in the case of a Timeout
var ErrorMQTTTimeout = fmt.Errorf("MQTT Timeout")
var clientDefaultTimeout = (5 * time.Second)

// MQTTClientConfig holds the essential elements needed to configure the MQTTClient
type MQTTClientConfig struct {
	Host                   string
	Port                   string
	RootCertFile           string
	PrivateKeyPEMFile      string
	ProjectID              string
	CloudRegion            string
	RegistryID             string
	DeviceID               string
	ReconnectRetryAttempts uint
	ReconnectRetryTimeout  time.Duration
	CommunicationAttempts  int
}

// MQTTTopics hold all the Topic names used by the client
type MQTTTopics struct {
	telemetryPublishTopic     string
	statePublishTopic         string
	configSubscriptionTopic   string
	commandsSubscriptionTopic string
}

// MQTTClient contains the essential elements that the MQTT client needs to function
type MQTTClient struct {
	Client                MQTT.Client
	Config                MQTTClientConfig
	messageQueue          *MessageQueue
	topics                MQTTTopics
	context               context.Context
	dataAvailable         chan bool
	communicationAttempts int
}

func connectionLostHandler(client MQTT.Client, err error) {
	connection := client.IsConnected() && client.IsConnectionOpen()
	fmt.Printf("Connection %t\n", connection)
	client.Disconnect(100)
	connection = client.IsConnected() && client.IsConnectionOpen()
	fmt.Printf("Connection %t\n", connection)
}

// NewMQTTClient intialises and returns a new instance of a MQTTClient.
func NewMQTTClient(ctx context.Context, cfg MQTTClientConfig, defaultHandler MQTT.MessageHandler, credentialsProvider MQTT.CredentialsProvider) (*MQTTClient, error) {
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

	if cfg.ReconnectRetryAttempts == 0 {
		cfg.ReconnectRetryAttempts = 3
	}

	if cfg.ReconnectRetryTimeout == 0 {
		cfg.ReconnectRetryTimeout = 5 * time.Second
	}

	if cfg.CommunicationAttempts == 0 {
		cfg.CommunicationAttempts = 3
	}

	mq, err := NewMessageQueue("mqtt-queue")
	if err != nil {
		return nil, errors.Wrap(err, "NewMessageQueue")
	}

	mc := &MQTTClient{
		Client:                MQTT.NewClient(opts),
		Config:                cfg,
		context:               ctx,
		messageQueue:          mq,
		dataAvailable:         make(chan bool),
		communicationAttempts: 0,
	}

	mc.topics = MQTTTopics{
		telemetryPublishTopic:     mc.formatMQTTTopicString("events"),
		statePublishTopic:         mc.formatMQTTTopicString("state"),
		configSubscriptionTopic:   mc.formatMQTTTopicString("config"),
		commandsSubscriptionTopic: mc.formatMQTTTopicString("commands/#"),
	}

	go mc.publishHandler()

	return mc, nil
}

func (mc *MQTTClient) publishHandler() {
	log.Println("[publishHandler] starting loop")
	for {
		select {
		case <-mc.context.Done():
			fmt.Println("[publishHandler] exiting loop")
			return
		case <-mc.dataAvailable:
			mc.publishAllAvailable()
		}
	}
}

func (mc *MQTTClient) publishAllAvailable() {
	log.Println("[publishAllAvailable] starting")
	err := retry.Do(
		func() error {
			if !mc.isConnectionGood() {
				err := mc.Connect()
				if err != nil {
					log.Printf("[publishAllAvailable] Error: %v\n", err)
					return ErrorNotConnected
				}
				if !mc.isConnectionGood() {
					log.Println("[publishAllAvailable] NOT CONNECTED")
					return ErrorNotConnected
				}
			}

			for {
				dst, ok, err := mc.messageQueue.FirstMessage()
				if !ok {
					if err != nil {
						return errors.Wrap(err, "FirstMessage error")
					}
					return nil
				}
				err = tokenChecker(mc.Client.Publish(dst.Topic, Qos, false, dst.Payload))
				if err != nil {
					log.Println("Error publishing queued message")
					return errors.Wrapf(err, "Publish error")
				}
				mc.messageQueue.RemoveFirstMessage()
			}
		},
		retry.Attempts(mc.Config.ReconnectRetryAttempts),
		retry.Delay(mc.Config.ReconnectRetryTimeout),
		retry.OnRetry(func(u uint, err error) {
			log.Printf("### [RetryFunction] instance number: %d. Error: %v", u, err)
		}),
	)
	if err != nil {
		mc.communicationAttempts++
	} else {
		mc.communicationAttempts = 0
	}
}

func tokenChecker(token MQTT.Token) error {
	okflag := token.WaitTimeout(clientDefaultTimeout)
	if !okflag {
		return ErrorMQTTTimeout
	}
	if token.Error() != nil {
		return token.Error()
	}
	return nil
}

// Connect attempts to connect to the MQTT Broker and returns an error if
// unsuccessful
func (mc *MQTTClient) Connect() error {
	err := tokenChecker(mc.Client.Connect())
	if err != nil {
		return errors.Wrap(err, "MQTTClient Connect error")
	}
	return nil
}

// Disconnect from the MQTT client
func (mc *MQTTClient) Disconnect() error {
	if mc == nil || !mc.isConnectionGood() {
		return ErrorNotConnected
	}

	log.Println("[MQTTClient] Disconnecting")
	mc.Client.Disconnect(250)
	return nil
}

func (mc *MQTTClient) isConnectionGood() bool {
	fmt.Printf("[isConnectionGood] IsConnected: %t IsConnectionOpen: %t", mc.Client.IsConnected(), mc.Client.IsConnectionOpen())
	return mc.Client.IsConnected() && mc.Client.IsConnectionOpen()
}

// IsConnectionGood returns true if connection open and connected
func (mc *MQTTClient) IsConnectionGood() bool {
	if mc == nil {
		return false
	}
	return mc.Config.CommunicationAttempts < mc.communicationAttempts
}

func (mc *MQTTClient) registerHandler(topic string, handler MQTT.MessageHandler) error {
	if mc == nil || !mc.isConnectionGood() {
		return ErrorNotConnected
	}
	return tokenChecker(mc.Client.Subscribe(topic, Qos, handler))
}

// RegisterConfigHandler registers the `handler` to the IOT cloud's device config topic
func (mc *MQTTClient) RegisterConfigHandler(handler MQTT.MessageHandler) error {
	return mc.registerHandler(mc.topics.configSubscriptionTopic, handler)
}

// RemoveConfigHandler removes the subscription for the Config topic and handler
func (mc *MQTTClient) RemoveConfigHandler() error {
	return tokenChecker(mc.Client.Unsubscribe(mc.topics.configSubscriptionTopic))
}

// RegisterCommandHandler registers the `handler` to the IOT cloud's device command topic
func (mc *MQTTClient) RegisterCommandHandler(handler MQTT.MessageHandler) error {
	return mc.registerHandler(mc.topics.commandsSubscriptionTopic, handler)
}

// RemoveCommandHandler removes the subscription for the Command topic and handler
func (mc *MQTTClient) RemoveCommandHandler() error {
	return tokenChecker(mc.Client.Unsubscribe(mc.topics.commandsSubscriptionTopic))
}

func (mc *MQTTClient) publish(topic string, payload []byte) {
	msg := Message{
		Topic:   topic,
		Payload: []byte{},
	}
	// append(a[:0], src...) is a safe copy
	msg.Payload = append(msg.Payload[:0], payload...)

	mc.messageQueue.QueueMessage(msg)
	fmt.Printf("Message queue size %d\n", mc.messageQueue.QueueSize())
	mc.dataAvailable <- true
}

// PublishTelemetryEvent sends the payload to the MQTT broker, if it doesnt
// work we get an error.
func (mc *MQTTClient) PublishTelemetryEvent(payload []byte) {
	mc.publish(mc.topics.telemetryPublishTopic, payload)
}

// PublishState sends the payload to the MQTT broker's Config topic.
func (mc *MQTTClient) PublishState(payload []byte) {
	mc.publish(mc.topics.statePublishTopic, payload)
}

func getMQTTClientID(cfg MQTTClientConfig) string {
	return fmt.Sprintf("projects/%s/locations/%s/registries/%s/devices/%s",
		cfg.ProjectID,
		cfg.CloudRegion,
		cfg.RegistryID,
		cfg.DeviceID)
}

func (mc *MQTTClient) formatMQTTTopicString(topic string) string {
	return fmt.Sprintf("/devices/%s/%s", mc.Config.DeviceID, topic)
}

func (mc *MQTTClient) formatMQTTPublishTopicString(topic string) string {
	return fmt.Sprintf("/projects/%s/topics/%s", mc.Config.ProjectID, topic)
}

func getMQTTBrokerAddress(cfg MQTTClientConfig) string {
	brokerAddress := fmt.Sprintf("ssl://%s:%s", cfg.Host, cfg.Port)
	log.Printf("[MQTTClient] getMQTTBrokerAddress %s", brokerAddress)
	return brokerAddress
}
