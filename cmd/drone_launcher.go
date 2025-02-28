package main

import (
	"context"
	"math/rand"
	"os"
	"sync"
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
	configmap := &kafka.ConfigMap{
		"bootstrap.servers": broker,
		"sasl.mechanisms":   "PLAIN",
		"security.protocol": "SASL_SSL",
		"sasl.username":     "$ConnectionString",
		"sasl.password":     os.Getenv("KAFKA_EVENTHUB_CONNECTION_STRING"),
	}

	var wg sync.WaitGroup
	for i := 0; i < getDroneInstances(c); i++ {
		wg.Add(1)
		go func(log *zap.Logger, configmap *kafka.ConfigMap, c *cli.Context, wg *sync.WaitGroup) {
			defer wg.Done()
			// Create new drone using the config map
			drone, err := service.NewDrone(log, configmap, getRegularMessageTopic(c), getAssistanceMessageTopic(c))
			if err != nil {
				log.Error("New drone", zap.Error(err))
			}
			// Close the drone's producer at the end before exiting
			defer drone.Close()

			// Start the drone service that will send messages regularly
			drone.Start(context.Background(), msgInterval)
		}(log, configmap, c, &wg)
	}

	wg.Wait()
}
