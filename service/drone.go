package service

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"image"
	"image/jpeg"
	"math/rand"
	"os"
	"strconv"
	"time"

	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/pkg/errors"
	"github.com/segmentio/ksuid"
	"go.uber.org/zap"
)

// Drone sends messages to the kafka stream
type Drone struct {
	id                 string
	log                *zap.Logger
	kafkaProducer      *kafka.Producer
	deliveryChan       chan kafka.Event
	regularMsgTopic    string
	assistanceMsgTopic string
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
func NewDrone(log *zap.Logger, configmap *kafka.ConfigMap, regularMsgTopic, assistanceMsgTopic string) (*Drone, error) {
	producer, err := kafka.NewProducer(configmap)
	if err != nil {
		return nil, errors.WithStack(err)
	}

	return &Drone{
		id:                 "DR-" + ksuid.New().String(),
		kafkaProducer:      producer,
		log:                log,
		deliveryChan:       make(chan kafka.Event),
		regularMsgTopic:    regularMsgTopic,
		assistanceMsgTopic: assistanceMsgTopic,
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
			value := rand.Int() % 10

			// 1 out of 10 messages is a message demanding assistance.
			// All other messages are regular messages.
			switch value {
			case 0:
				if err := d.sendAssistanceMessage(); err != nil {
					d.log.Error("send regular message", zap.Error(err))
				}
			default:
				if err := d.sendMessage(); err != nil {
					d.log.Error("send assistance message", zap.Error(err))
				}
			}
		}
	}
}

// Close the drone's kafka Producer
func (d *Drone) Close() {
	d.kafkaProducer.Close()
	close(d.deliveryChan)
}

func (d *Drone) sendMessage() error {
	// Once every 2 regular messages, an violation is detected and sent
	// If so, we set the violation code, the image ID, and send the image
	isViolation := false
	if rand.Int()%2 == 0 {
		isViolation = true
	}

	// Generate violation code and image id
	violationCode := rand.Intn(99) + 1
	imgID := fmt.Sprintf("img-%d-%s-%s", violationCode, ksuid.New().String(), d.id)

	// Generate random street code
	location := rand.Int63n(89999) + 10000
	msg := &Message{
		DroneID:  d.id,
		Time:     time.Now(),
		Location: strconv.FormatInt(location, 10),
	}
	if isViolation {
		msg.ImageID = imgID
		msg.ViolationCode = violationCode
	}

	msgByte, err := json.Marshal(msg)
	if err != nil {
		return errors.WithStack(err)
	}

	// If we need to send the image (isViolation is true), we send it
	if isViolation {
		randomImgIndex := rand.Intn(20) + 1
		fImg, err := os.Open(fmt.Sprintf("../data/%d.jpg", randomImgIndex))
		if err != nil {
			return errors.WithMessage(err, "load image")
		}
		defer fImg.Close()
		img, _, err := image.Decode(fImg)
		if err != nil {
			return errors.WithMessage(err, "decode image")
		}
		imgBuf := new(bytes.Buffer)
		if err := jpeg.Encode(imgBuf, img, nil); err != nil {
			return errors.WithStack(err)
		}

		d.log.Info("Produce image",
			zap.String("image id", imgID),
		)
		err = d.kafkaProducer.Produce(&kafka.Message{
			TopicPartition: kafka.TopicPartition{Topic: &d.regularMsgTopic, Partition: kafka.PartitionAny},
			Headers: []kafka.Header{
				kafka.Header{
					Key:   "type",
					Value: []byte("image"),
				},
				kafka.Header{
					Key:   "id",
					Value: []byte(imgID),
				},
			},
			Value: imgBuf.Bytes(),
		}, d.deliveryChan)
	}

	// Produce message to regular message topic
	d.log.Info("Produce regular message",
		zap.String("drone id", d.id),
		zap.String("location", msg.Location),
	)
	err = d.kafkaProducer.Produce(&kafka.Message{
		TopicPartition: kafka.TopicPartition{Topic: &d.regularMsgTopic, Partition: kafka.PartitionAny},
		Headers: []kafka.Header{
			kafka.Header{
				Key:   "type",
				Value: []byte("message"),
			},
		},
		Value: msgByte,
	}, d.deliveryChan)
	if err != nil {
		return errors.WithStack(err)
	}

	e := <-d.deliveryChan
	if m := e.(*kafka.Message); m.TopicPartition.Error != nil {
		return errors.WithStack(m.TopicPartition.Error)
	}

	return nil
}

func (d *Drone) sendAssistanceMessage() error {
	msg := &Message{
		DroneID:  d.id,
		Time:     time.Now(),
		Location: "NYC",
	}
	msgByte, err := json.Marshal(msg)
	if err != nil {

	}

	// Produce message to regular message topic
	d.log.Info("Produce assistance message",
		zap.String("drone id", d.id),
		zap.String("location", msg.Location),
	)
	err = d.kafkaProducer.Produce(&kafka.Message{
		TopicPartition: kafka.TopicPartition{Topic: &d.assistanceMsgTopic, Partition: kafka.PartitionAny},
		Value:          msgByte,
	}, d.deliveryChan)
	if err != nil {
		return errors.WithStack(err)
	}

	e := <-d.deliveryChan
	if m := e.(*kafka.Message); m.TopicPartition.Error != nil {
		return errors.WithStack(m.TopicPartition.Error)
	}

	return nil
}

func (d *Drone) randomDuration(duration time.Duration) time.Duration {
	// The returned duration is a random between [duration - duration/3, duration + duration/3],
	// with a low bound (lb) and high bound (hb)
	// So it is rand.Intn(hb - lb) + lb = rand.Intn(2/3 * duration) + 2/3 * duration
	return time.Duration(rand.Int63n(2*int64(duration)/3) + 2*int64(duration)/3)
}
