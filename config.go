package main

import (
	"fmt"
	"os"

	mqtt "github.com/8power/gcf-mqtt-client/mqtt"
)

const (
	host          = "mqtt.googleapis.com"
	port          = "8883"
	projectID     = "skillful-mason-244208"
	cloudRegion   = "europe-west1"
	registryID    = "vibration-energy-harvesting-registry"
	deviceID      = "Gateway-F359P42"
	telemetryPipe = "telemetry.pipe"
	statePipe     = "state.pipe"
	configPipe    = "config.pipe"
)

func createConfig() mqtt.ClientConfig {
	cfg := mqtt.ClientConfig{}
	home := os.Getenv("HOME")

	cfg.Host = os.Getenv("HOST")
	if cfg.Host == "" {
		cfg.Host = host
	}

	cfg.Port = os.Getenv("PORT")
	if cfg.Port == "" {
		cfg.Port = port
	}

	cfg.RootCertFile = os.Getenv("ROOTS_CERT")
	if cfg.RootCertFile == "" {
		cfg.RootCertFile = fmt.Sprintf("%s/.certs/roots.pem", home)
	}

	cfg.PrivateKeyPEMFile = os.Getenv("PEM_FILE")
	if cfg.PrivateKeyPEMFile == "" {
		cfg.PrivateKeyPEMFile = fmt.Sprintf("%s/.certs/iot_rsa_private.pem", home)
	}

	cfg.ProjectID = os.Getenv("PROJECT_ID")
	if cfg.ProjectID == "" {
		cfg.ProjectID = projectID
	}

	cfg.CloudRegion = os.Getenv("CLOUD_REGION")
	if cfg.CloudRegion == "" {
		cfg.CloudRegion = cloudRegion
	}

	cfg.RegistryID = os.Getenv("REGISTRY_ID")
	if cfg.RegistryID == "" {
		cfg.RegistryID = registryID
	}

	cfg.DeviceID = os.Getenv("DEVICE_ID")
	if cfg.DeviceID == "" {
		cfg.DeviceID = deviceID
	}
	return cfg
}
