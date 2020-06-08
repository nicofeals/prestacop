package service

import (
	"context"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"os"
	"strconv"
	"time"

	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/pkg/errors"
	"github.com/segmentio/ksuid"
	"go.uber.org/zap"
)

// CsvSender sends lines of a csv as messages to a kafka stream (or EventHubs)
type CsvSender struct {
	id            string
	log           *zap.Logger
	kafkaProducer *kafka.Producer
	topic         string
	reader        *csv.Reader
}

// NewCsvSender initializes a new CsvSender instance with a kafka producer (and a logger)
// filename is the name of the csv file to load and use to send messages.
func NewCsvSender(log *zap.Logger, configmap *kafka.ConfigMap, topic, filename string) (*CsvSender, error) {
	producer, err := kafka.NewProducer(configmap)
	if err != nil {
		return nil, errors.WithStack(err)
	}

	csvFile, err := os.Open(filename)
	if err != nil {
		return nil, errors.WithStack(err)
	}

	r := csv.NewReader(csvFile)

	return &CsvSender{
		id:            "CSV-" + ksuid.New().String(),
		kafkaProducer: producer,
		log:           log,
		topic:         topic,
		reader:        r,
	}, nil
}

// Start the CsvSender service
func (s *CsvSender) Start(ctx context.Context) {
	s.log.Info("Starting csv sender service")

	isFirstLine := true
	for {
		// If it's the first line, we want to skip it because it corresponds to the column titles
		if isFirstLine {
			_, err := s.reader.Read()
			if err != nil {
				s.log.Error("read new csv line",
					zap.Error(err),
				)
			}
		}

		record, err := s.reader.Read()
		if err != nil {
			s.log.Error("read new csv line",
				zap.Error(err),
			)
		}

		if err := s.sendMessage(record); err != nil {
			s.log.Error("send csv line",
				zap.Error(err),
			)
		}
	}
}

// Close kafka producer
func (s *CsvSender) Close() {
	s.kafkaProducer.Close()
}

func (s *CsvSender) sendMessage(record []string) error {
	println(record[0])
	violationCode, err := strconv.ParseInt(record[5], 10, 0)
	if err != nil {
		return errors.WithStack(err)
	}

	lat, long, err := randomLocation(record[9])
	if err != nil {
		return errors.WithStack(err)
	}

	t := "00:00AM"
	if len(record[19]) == 5 {
		t = fmt.Sprintf("%s:%sM", record[19][:2], record[19][2:])
	}
	issueDate, err := time.Parse("01/02/2006 03:04PM", fmt.Sprintf("%s %s", record[4], t))
	if err != nil {
		return errors.WithStack(err)
	}

	msg := &DroneMessage{
		DroneID:            s.id,
		Latitude:           lat,
		Longitude:          long,
		RequiresAssistance: false,
		Time:               issueDate,
		ViolationCode:      int(violationCode),
	}

	msgByte, err := json.Marshal(msg)
	if err != nil {
		return errors.WithStack(err)
	}

	s.log.Info("Produce csv message",
		zap.String("drone id", s.id),
		zap.Time("time", msg.Time),
		zap.Float64("latitude", msg.Latitude),
		zap.Float64("longitude", msg.Longitude),
		zap.Int("violation code", msg.ViolationCode),
		zap.String("image id", msg.ImageID),
	)

	err = s.kafkaProducer.Produce(&kafka.Message{
		TopicPartition: kafka.TopicPartition{Topic: &s.topic, Partition: kafka.PartitionAny},
		Value:          msgByte,
	}, nil)
	if err != nil {
		return errors.WithMessage(err, "produce csv message")
	}

	e := <-s.kafkaProducer.Events()

	if ke, ok := e.(kafka.Error); ok {
		s.log.Error("message",
			zap.Any("code", ke.Code()),
			zap.String("error", ke.String()),
		)
		return errors.New("produce csv message")
	}

	return nil
}
