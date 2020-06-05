package service

import (
	"context"
	"encoding/json"
	"fmt"
	"math/rand"
	"strconv"
	"time"

	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/pkg/errors"
	"github.com/segmentio/ksuid"
	"go.uber.org/zap"
)

// Drone sends messages to the kafka stream
type Drone struct {
	id            string
	log           *zap.Logger
	kafkaProducer *kafka.Producer
	topic         string
}

// Message contains the information sent by the drone
type Message struct {
	Location           string    `json:"location"`
	Time               time.Time `json:"time"`
	DroneID            string    `json:"drone-id"`
	ViolationCode      int       `json:"violation-code"`
	ImageID            string    `json:"image-id"`
	RequiresAssistance bool      `json:"requires-assistance"`
}

// NewDrone initializes a new Drone instance with a kafka producer (and a logger)
func NewDrone(log *zap.Logger, configmap *kafka.ConfigMap, topic string) (*Drone, error) {
	producer, err := kafka.NewProducer(configmap)
	if err != nil {
		return nil, errors.WithStack(err)
	}

	return &Drone{
		id:            "DR-" + ksuid.New().String(),
		kafkaProducer: producer,
		log:           log,
		topic:         topic,
	}, nil
}

// Start launches the service and regularly sends messages
func (d *Drone) Start(ctx context.Context, msgInterval time.Duration) {
	d.log.Info("Starting drone service",
		zap.Duration("message interval", msgInterval),
	)

	for {
		select {
		case <-ctx.Done():
			return
		case <-time.After(d.randomDuration(msgInterval)):
			if err := d.sendMessage(); err != nil {
				d.log.Error("send message", zap.Error(err))
			}
		}
	}
}

// Close the drone's kafka Producer
func (d *Drone) Close() {
	d.kafkaProducer.Close()
}

func (d *Drone) sendMessage() error {
	// 1 out of 10 times, a message is an assistance required message
	value := rand.Int() % 10
	requiresAssistance := false
	if value == 0 {
		requiresAssistance = true
	}

	// Once every 2 regular messages, an violation is detected and sent
	// If so, we set the violation code and the image ID
	isViolation := false
	if rand.Int()%2 == 0 {
		isViolation = true
	}

	// Generate violation code and image id
	violationCode := rand.Intn(99) + 1
	imgID := fmt.Sprintf("img-%d-%s-%s", violationCode, d.id, ksuid.New().String())

	// Generate random street code
	location := rand.Int63n(89999) + 10000
	msg := &Message{
		DroneID:            d.id,
		Time:               time.Now(),
		Location:           strconv.FormatInt(location, 10),
		RequiresAssistance: requiresAssistance,
	}

	if isViolation {
		msg.ImageID = imgID
		msg.ViolationCode = violationCode
	}

	msgByte, err := json.Marshal(msg)
	if err != nil {
		return errors.WithStack(err)
	}

	// Produce message to concerned topic
	if !requiresAssistance {
		d.log.Info("Produce regular message",
			zap.String("drone id", d.id),
			zap.String("location", msg.Location),
			zap.Int("violation code", msg.ViolationCode),
			zap.String("image id", msg.ImageID),
		)
	} else {
		d.log.Info("Produce assistance message",
			zap.String("drone id", d.id),
			zap.String("location", msg.Location),
			zap.Int("violation code", msg.ViolationCode),
			zap.String("image id", msg.ImageID),
		)
	}

	err = d.kafkaProducer.Produce(&kafka.Message{
		TopicPartition: kafka.TopicPartition{Topic: &d.topic, Partition: kafka.PartitionAny},
		Value:          msgByte,
	}, nil)
	if err != nil {
		return errors.WithMessage(err, "produce message")
	}

	e := <-d.kafkaProducer.Events()

	if ke, ok := e.(kafka.Error); ok {
		d.log.Error("message",
			zap.Any("code", ke.Code()),
			zap.String("error", ke.String()),
		)
		return errors.New("produce message")
	}

	return nil
}

func (d *Drone) randomDuration(duration time.Duration) time.Duration {
	// The returned duration is a random between [duration - duration/3, duration + duration/3],
	// with a low bound (lb) and high bound (hb)
	// So it is rand.Intn(hb - lb) + lb = rand.Intn(2/3 * duration) + 2/3 * duration
	return time.Duration(rand.Int63n(2*int64(duration)/3) + 2*int64(duration)/3)
}
