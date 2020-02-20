package main

import (
	"context"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"time"

	mq "github.com/8power/gcf-mqtt-client"
	jwtGo "github.com/dgrijalva/jwt-go"
	MQTT "github.com/eclipse/paho.mqtt.golang"
	"github.com/pkg/errors"
)

var RootCertFile string
var PrivateKeyPEMFile string

const (
	Host        = "mqtt.googleapis.com"
	Port        = "8883"
	ProjectID   = "skillful-mason-244208"
	CloudRegion = "europe-west1"
	RegistryID  = "vibration-energy-harvesting-registry"
	DeviceID    = "Gateway-F359P42"
)

func GetClientConfig() mq.ClientConfig {
	home := os.Getenv("HOME")
	return mq.ClientConfig{
		Host:                   Host,
		Port:                   Port,
		RootCertFile:           fmt.Sprintf("%s/.certs/roots.pem", home),
		PrivateKeyPEMFile:      fmt.Sprintf("%s/.certs/iot_rsa_private.pem", home),
		ProjectID:              ProjectID,
		CloudRegion:            CloudRegion,
		RegistryID:             RegistryID,
		DeviceID:               DeviceID,
		ReconnectRetryAttempts: 5,
		ReconnectRetryTimeout:  5 * time.Second,
		CommunicationAttempts:  5,
	}
}

func main() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	spec := mq.NewMQTTClientConfig{
		Context:              ctx,
		ClientConfig:         GetClientConfig(),
		DefaultMessageHander: testHander,
		CredentialsProvider:  credentialsProvider,
		OnConnectFunc: func(c *mq.MQTTClient) error {
			err := c.RegisterConfigHandler(func(client MQTT.Client, msg MQTT.Message) {
				fmt.Printf("[config handler] Topic: %v\n", msg.Topic())
				fmt.Printf("[config handler] Payload: %s\n", msg.Payload())
			})
			if err != nil {
				return errors.Wrap(err, "RegisterConfigHandler error")
			}

			err = c.RegisterCommandHandler(func(client MQTT.Client, msg MQTT.Message) {
				fmt.Printf("[command handler] Topic: %v\n", msg.Topic())
				fmt.Printf("[command handler] Payload: %s\n", msg.Payload())
			})
			if err != nil {
				return errors.Wrap(err, "RegisterCommandHandler error")
			}
			return nil
		},
	}

	mc, err := mq.NewMQTTClient(spec)
	if err != nil {
		log.Fatalf("Error raised in NewMQTTClient: %v\n", err)
	}
	fmt.Printf("NewMQTTClient %v\nConnecting...\n", mc)

	err = mc.Connect()
	if err != nil {
		log.Fatalf("Connect error: %v\n", err)
	}

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
	fmt.Printf("[credentialsProvider] creating new Credentials\n")
	username = "unused"
	password, err := createJWT(ProjectID, PrivateKeyPEMFile)
	if err != nil {
		fmt.Printf("[credentialsProvider] Error creating JWT %v", err)
	}
	fmt.Printf("[credentialsProvider] returning %s\n%s\n", username, password)
	return username, password
}

func testHander(client MQTT.Client, msg MQTT.Message) {
	fmt.Printf("[handler] Topic: %v\n", msg.Topic())
	fmt.Printf("[handler] Payload: %v\n", msg.Payload())
}
