# gcf-mqtt-client
MQTT functions to communicate with the GCF IoT Core MQTT broker

Shared by rf-gateway and the GCP instance.


openssl req -x509 -nodes -newkey rsa:2048 -keyout ~/.certs/iot_rsa.key.pem -out ~/.certs/iot_rsa.crt.pem -days 365 -subj "/CN=unused"


const (
	Host              = "mqtt.googleapis.com"
	Port              = "8883"
	RootCertFile      = "/home/rob/.certs/roots.pem"
	PrivateKeyPEMFile = "/home/rob/.certs/iot_rsa.key.pem"
	ProjectID         = "vibration-energy-harvesting"
	CloudRegion       = "europe-west1"
	RegistryID        = "Vibration Energy Harvesting"
	DeviceID          = "dell-development-laptop"
)

gcloud iot registries describe skillful-mason-244208 --project=vibration-energy-harvesting --region=europe-west1