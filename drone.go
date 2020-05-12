package main

import (
	"time"

	"github.com/pkg/errors"
	"go.uber.org/zap"
	"gopkg.in/confluentinc/confluent-kafka-go.v1/kafka"
)

// Drone sends messages to the kafka stream
type Drone struct {
	log *zap.Logger
	kp  *kafka.Producer
}

// Message contains the information sent by the drone
type Message struct {
	Location string
	Time     time.Time
	DroneID  string
}

// NewDrone initializes a new Drone instance with a kafka producer (and a logger)
func NewDrone(log *zap.Logger, configmap *kafka.ConfigMap) (*Drone, error) {
	producer, err := kafka.NewProducer(configmap)
	if err != nil {
		return nil, errors.WithStack(err)
	}

	return &Drone{
		kp:  producer,
		log: log,
	}, nil
}

// Start launches the service and regularly sends messages
func (*Drone) Start() {}

func (d *Drone) sendMessage() {}

func (d *Drone) sendAssistanceMessage() {}
