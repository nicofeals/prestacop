package main

import (
	"math/rand"
	"os"
	"time"

	"github.com/nicofeals/prestacop-drone/config"
	"go.uber.org/zap"
	"gopkg.in/confluentinc/confluent-kafka-go.v1/kafka"
)

func main() {
	rand.Seed(time.Now().UnixNano())
	log := config.NewZapLogger("info")

	if len(os.Args) != 3 {
		log.Error("Usage: %s <broker> <topic>\n", os.Args[0])
		os.Exit(1)
	}

	// Config map used for the kafka producer
	configmap := &kafka.Configmap{"bootstrap.servers": "localhost"}

	// Create new drone using the config map
	drone, err := NewDrone(log, configmap)
	if err != nil {
		log.Error("New drone", zap.Error(err))
	}

	// Start the drone service that will send messages regularly
	drone.Start()

	// Close the drone's producer at the end before exiting
	defer drone.kp.Close()
}
