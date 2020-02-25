package main

import (
	"context"
	"io/ioutil"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	mqtt "github.com/8power/gcf-mqtt-client/mqtt"
	gatewaypipes "github.com/8power/named-pipe/gateway-pipes"
	jwtGo "github.com/dgrijalva/jwt-go"
	MQTT "github.com/eclipse/paho.mqtt.golang"
	"github.com/pkg/errors"
)

var configuration mqtt.ClientConfig
var signals chan os.Signal
var mqttClient *mqtt.MQTTClient
var pipes *gatewaypipes.Pipes

func init() {
	configuration = createConfig()
	signals = make(chan os.Signal, 1)
	signal.Notify(signals, syscall.SIGINT, syscall.SIGTERM)
	p, err := gatewaypipes.New()
	if err != nil {
		log.Fatalf("Named Pipes error %v", err)
	}
	pipes = p
}

func exitHandler() {
	log.Println("[exitHandler] starting")
	pipes.Close()

	if mqttClient != nil {
		err := mqttClient.ClientConnect()
		if err != nil {
			log.Printf("mqttClient.Disconnect error %v\n", err)
		}
	}
	log.Println("[exitHandler] complete")
}

func main() {
	log.Println("[gcf-mqtt-client] service starting")
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	spec := mqtt.NewMQTTClientConfig{
		Context:              ctx,
		ClientConfig:         configuration,
		DefaultMessageHander: defaultHandler,
		CredentialsProvider:  credentialsProvider,
		OnConnectFunc:        onConnect,
	}

	err := mqtt.NewMQTTClient(spec)
	if err != nil {
		log.Fatalf("Error raised in NewMQTTClient: %v\n", err)
	}

	log.Println("[gcf-mqtt-client] connecting")
	err = mqtt.MqttClient.ClientConnect()
	if err != nil {
		log.Fatalf("Connect error: %v\n", err)
	}

	log.Println("[gcf-mqtt-client] service running...")
	setupPipeHandlers(mqtt.MqttClient)

	select {
	case <-signals:
		exitHandler()
		os.Exit(0)
	}
}

func setupPipeHandlers(mc *mqtt.MQTTClient) {
	go pipes.Telemetry.Read(func(b []byte) {
		log.Printf("[Telemetry] publishing event\n")
		mc.PublishTelemetryEvent(b)
	})

	go pipes.State.Read(func(b []byte) {
		log.Printf("[State] publishing state\n")
		mc.PublishState(b)
	})
}

func defaultHandler(client MQTT.Client, msg MQTT.Message) {
	log.Printf("ERROR Unhandled message received\n")
	log.Printf("[defaultHandler] Topic: %v\n", msg.Topic())
	log.Printf("[defaultHandler] Payload: %v\n", msg.Payload())
}

func onConnect(client *mqtt.MQTTClient) error {
	err := client.RegisterConfigHandler(func(client MQTT.Client, msg MQTT.Message) {
		pipes.Config.Write(msg.Payload())
	})
	if err != nil {
		return errors.Wrap(err, "RegisterConfigHandler error")
	}

	err = client.RegisterCommandHandler(func(client MQTT.Client, msg MQTT.Message) {
		pipes.Command.Write(msg.Payload())
	})
	if err != nil {
		return errors.Wrap(err, "RegisterCommandHandler error")
	}
	return nil
}

func credentialsProvider() (username string, password string) {
	log.Println("[credentialsProvider] creating new credentials")
	username = "unused"
	password, err := createJWT()
	if err != nil {
		log.Printf("[credentialsProvider] Error creating JWT %v\n", err)
	}
	return username, password
}

// CreateJWT uses the projectId and privateKeyFile to make a Java Web Token (JWT)
func createJWT() (string, error) {
	keyBytes, err := ioutil.ReadFile(configuration.PrivateKeyPEMFile)
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
		Audience:  configuration.ProjectID,
	}

	token := jwtGo.NewWithClaims(jwtGo.SigningMethodRS256, claims)
	return token.SignedString(privateKey)
}
