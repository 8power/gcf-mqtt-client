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

// ClientConfig holds the essential elements needed to configure the MQTTClient
type ClientConfig struct {
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

// MQTTConnectFunction is called when a connection has occured.
type MQTTConnectFunction func(*MQTTClient) error

// MQTTClient contains the essential elements that the MQTT client needs to function
type MQTTClient struct {
	Client                MQTT.Client
	Config                ClientConfig
	OnConnectFunc         MQTTConnectFunction
	messageQueue          *MessageQueue
	topics                MQTTTopics
	context               context.Context
	dataAvailable         chan bool
	communicationAttempts int
}

// NewMQTTClientConfig holds the elements required to create the client
type NewMQTTClientConfig struct {
	Context              context.Context
	ClientConfig         ClientConfig
	DefaultMessageHander MQTT.MessageHandler
	CredentialsProvider  MQTT.CredentialsProvider
	OnConnectFunc        MQTTConnectFunction
}

func connectionLostHandler(client MQTT.Client, err error) {
	fmt.Printf("[connectionLostHandler] starting")
	connection := client.IsConnected() && client.IsConnectionOpen()
	fmt.Printf("Connection %t\n", connection)
	client.Disconnect(100)
	connection = client.IsConnected() && client.IsConnectionOpen()
	fmt.Printf("Connection %t\n", connection)
}

// NewMQTTClient intialises and returns a new instance of a MQTTClient.
func NewMQTTClient(spec NewMQTTClientConfig) (*MQTTClient, error) {
	certpool := x509.NewCertPool()
	pemCerts, err := ioutil.ReadFile(spec.ClientConfig.RootCertFile)
	if err != nil {
		return nil, errors.Wrap(err, "Failed to read Root Cert File")
	}

	certpool.AppendCertsFromPEM(pemCerts)

	tlscfg := &tls.Config{
		RootCAs:            certpool,
		ClientAuth:         tls.NoClientCert,
		ClientCAs:          nil,
		InsecureSkipVerify: true,
		Certificates:       []tls.Certificate{},
		MinVersion:         tls.VersionTLS12,
	}

	if spec.ClientConfig.ReconnectRetryAttempts == 0 {
		spec.ClientConfig.ReconnectRetryAttempts = 3
	}

	if spec.ClientConfig.ReconnectRetryTimeout == 0 {
		spec.ClientConfig.ReconnectRetryTimeout = 5 * time.Second
	}

	if spec.ClientConfig.CommunicationAttempts == 0 {
		spec.ClientConfig.CommunicationAttempts = 3
	}

	opts := MQTT.NewClientOptions()
	opts = opts.AddBroker(getMQTTBrokerAddress(spec.ClientConfig))
	opts = opts.SetClientID(getMQTTClientID(spec.ClientConfig))
	opts = opts.SetTLSConfig(tlscfg)
	opts = opts.SetAutoReconnect(true)
	opts = opts.SetConnectionLostHandler(connectionLostHandler)
	opts = opts.SetCredentialsProvider(spec.CredentialsProvider)
	opts = opts.SetDefaultPublishHandler(spec.DefaultMessageHander)

	mq, err := NewMessageQueue("mqtt-queue")
	if err != nil {
		return nil, errors.Wrap(err, "NewMessageQueue")
	}

	mc := &MQTTClient{
		Client:                MQTT.NewClient(opts),
		Config:                spec.ClientConfig,
		OnConnectFunc:         spec.OnConnectFunc,
		context:               spec.Context,
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

func (mc *MQTTClient) isConnected() bool {
	if mc == nil {
		return false
	}
	return mc.Client.IsConnected() && mc.Client.IsConnectionOpen()
}

func (mc *MQTTClient) publishAllAvailable() {
	log.Println("[publishAllAvailable] starting")
	err := retry.Do(
		func() error {
			if !mc.isConnected() {
				err := mc.Connect()
				if err != nil {
					log.Printf("[publishAllAvailable] Error: %v\n", err)
					return ErrorNotConnected
				}
				if !mc.isConnected() {
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
	log.Println("[publishAllAvailable] complete")
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

const mqttStandOff = 250 * time.Millisecond

// Connect attempts to connect to the MQTT Broker and returns an error if
// unsuccessful
func (mc *MQTTClient) Connect() error {
	if mc == nil {
		return ErrorNotConnected
	}

	log.Printf("[Connect] starting")
	defer log.Printf("[Connect] complete")

	err := tokenChecker(mc.Client.Connect())
	if err != nil {
		return errors.Wrap(err, "MQTTClient Connect error")
	}

	retry.Do(func() error {
		if !mc.isConnected() {
			return ErrorNotConnected
		}
		return nil
	},
		retry.Attempts(4),
		retry.Delay(mqttStandOff),
		retry.OnRetry(func(u uint, err error) {
			log.Printf("[Connect] retesting connection attempt: %d", u)
		}),
	)
	err = mc.OnConnectFunc(mc)
	if err != nil {
		return errors.Wrap(err, "onConnected callback error")
	}
	return nil
}

// Disconnect from the MQTT client
func (mc *MQTTClient) Disconnect() error {
	if mc == nil || !mc.isConnected() {
		return ErrorNotConnected
	}

	log.Println("[Disconnect] starting")
	mc.Client.Disconnect(250)
	log.Println("[Disconnect] complete")
	return nil
}

// IsConnectionGood returns true if connection open and connected
func (mc *MQTTClient) IsConnectionGood() bool {
	if mc == nil {
		log.Println("[IsConnectionGood] mc == nil")
		return false
	}

	if !mc.isConnected() {
		log.Println("[IsConnectionGood] isConnected false")
		return false
	}

	return mc.communicationAttempts < mc.Config.CommunicationAttempts
}

func (mc *MQTTClient) registerHandler(topic string, handler MQTT.MessageHandler) error {
	if mc == nil || !mc.isConnected() {
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

func (mc *MQTTClient) publish(topic string, payload []byte) error {
	msg := Message{
		Topic:   topic,
		Payload: []byte{},
	}
	// append(a[:0], src...) is a safe copy
	msg.Payload = append(msg.Payload[:0], payload...)

	err := mc.messageQueue.QueueMessage(msg)
	if err != nil {
		return errors.Wrap(err, "QueueMessage error")
	}
	fmt.Printf("Message queue size %d\n", mc.messageQueue.QueueSize())
	mc.dataAvailable <- true
	return nil
}

// PublishTelemetryEvent sends the payload to the MQTT broker, if it doesnt
// work we get an error.
func (mc *MQTTClient) PublishTelemetryEvent(payload []byte) error {
	err := mc.publish(mc.topics.telemetryPublishTopic, payload)
	if err != nil {
		return errors.Wrap(err, "publish error")
	}
	return nil
}

// PublishState sends the payload to the MQTT broker's Config topic.
func (mc *MQTTClient) PublishState(payload []byte) error {
	err := mc.publish(mc.topics.statePublishTopic, payload)
	if err != nil {
		return errors.Wrap(err, "publish error")
	}
	return nil
}

func getMQTTClientID(cfg ClientConfig) string {
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

func getMQTTBrokerAddress(cfg ClientConfig) string {
	brokerAddress := fmt.Sprintf("ssl://%s:%s", cfg.Host, cfg.Port)
	log.Printf("[MQTTClient] getMQTTBrokerAddress %s", brokerAddress)
	return brokerAddress
}
