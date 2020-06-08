package main

import (
	"context"
	"math/rand"
	"os"
	"time"

	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/nicofeals/prestacop/config"
	"github.com/nicofeals/prestacop/service"
	"github.com/urfave/cli"
	"go.uber.org/zap"
)

func launchCsvSender(c *cli.Context) {
	rand.Seed(time.Now().UnixNano())
	env := getEnvironment(c)
	log := config.NewZapLogger(getLogLevel(c))

	log.Info("loading config",
		zap.String("env", env),
	)

	broker := getBroker(c)

	// Config map used for the kafka producer
	configmap := &kafka.ConfigMap{
		"bootstrap.servers": broker,
		"sasl.mechanisms":   "PLAIN",
		"security.protocol": "SASL_SSL",
		"sasl.username":     "$ConnectionString",
		"sasl.password":     os.Getenv("KAFKA_EVENTHUB_CONNECTION_STRING"),
	}

	// Create new csv sender using the config map
	csvSender, err := service.NewCsvSender(log, configmap, getRegularMessageTopic(c), getCSVpath(c))
	if err != nil {
		log.Error("New drone", zap.Error(err))
	}
	// Close the csv sender's producer at the end before exiting
	defer csvSender.Close()

	// Start the csv sender service that will send lines of the given csv as messages
	csvSender.Start(context.Background())
}
