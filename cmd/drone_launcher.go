package main

import (
	"context"
	"math/rand"
	"time"

	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/nicofeals/prestacop/config"
	"github.com/nicofeals/prestacop/service"
	"github.com/urfave/cli"
	"go.uber.org/zap"
)

func launchDrone(c *cli.Context) {
	rand.Seed(time.Now().UnixNano())
	env := getEnvironment(c)
	log := config.NewZapLogger(getLogLevel(c))

	log.Info("loading config",
		zap.String("env", env),
	)

	broker := getBroker(c)
	msgInterval := getMessageInterval(c)

	// Config map used for the kafka producer
	configmap := &kafka.ConfigMap{"bootstrap.servers": broker}

	// Create new drone using the config map
	drone, err := service.NewDrone(log, configmap)
	if err != nil {
		log.Error("New drone", zap.Error(err))
	}
	// Close the drone's producer at the end before exiting
	defer drone.Close()

	// Start the drone service that will send messages regularly
	if err := drone.Start(context.Background(), msgInterval); err != nil {
		log.Error("start drone", zap.Error(err))
	}
}
