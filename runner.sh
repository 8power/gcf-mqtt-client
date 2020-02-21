export HOST=mqtt.googleapis.com
export PORT=8883
export PROJECT_ID=skillful-mason-244208 
export CLOUD_REGION=europe-west1
export REGISTRY_ID=vibration-energy-harvesting-registry
export DEVICE_ID=pc-$(hostname)
export ROOTS_CERT=/home/${USER}/.certs/roots.pem
export PEM_FILE=/home/${USER}/.certs/iot_rsa_private.pem
./gcf-mqtt-client



