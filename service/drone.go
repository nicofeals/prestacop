package service

import (
	"context"
	"math/rand"
	"time"

	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/pkg/errors"
	"go.uber.org/zap"
)

// Drone sends messages to the kafka stream
type Drone struct {
	log *zap.Logger
	kp  *kafka.Producer
}

// Message contains the information sent by the drone
type Message struct {
	Location      string    `json:"location"`
	Time          time.Time `json:"time"`
	DroneID       string    `json:"drone-id"`
	ViolationCode int       `json:"violation-code"`
	ImageID       string    `json:"image-id"`
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
func (d *Drone) Start(ctx context.Context, msgInterval time.Duration) error {
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-time.After(d.randomDuration(msgInterval)):
			value := rand.Int() % 10

			// 1 out of 10 messages is a message demanding assistance.
			// All other messages are regular messages.
			switch value {
			case 0:
				if err := d.sendAssistanceMessage(); err != nil {
					return errors.WithStack(err)
				}
			default:
				if err := d.sendMessage(); err != nil {
					return errors.WithStack(err)
				}
			}
		}
	}
}

// Close the drone's kafka Producer
func (d *Drone) Close() {
	d.kp.Close()
}

func (d *Drone) sendMessage() error {

	return nil
}

func (d *Drone) sendAssistanceMessage() error {
	return nil
}

func (d *Drone) randomDuration(duration time.Duration) time.Duration {
	// The returned duration is a random between [duration - duration/3, duration + duration/3],
	// with a low bound (lb) and high bound (hb)
	// So it is rand.Intn(hb - lb) + lb = rand.Intn(2/3 * duration) + 2/3 * duration
	return time.Duration(rand.Int63n(2*int64(duration)/3) + 2*int64(duration)/3)
}
