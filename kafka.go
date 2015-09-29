/*
* Copyright (C) 2015 Alexey Gladkov <gladkov.alexey@gmail.com>
*
* This file is covered by the GNU General Public License,
* which should be included with kafka-http-proxy as the file COPYING.
 */

package main

import (
	"github.com/optiopay/kafka"
	"github.com/optiopay/kafka/proto"

	log "github.com/Sirupsen/logrus"

	"fmt"
	"time"
)

var (
	// KafkaOffsetNewest is a wrapper over kafka.StartOffsetNewest
	KafkaOffsetNewest = kafka.StartOffsetNewest

	// KafkaOffsetOldest is a wrapper over kafka.StartOffsetOldest
	KafkaOffsetOldest = kafka.StartOffsetOldest

	// KafkaErrReplicaNotAvailable is a wrapper over proto.ErrReplicaNotAvailable
	KafkaErrReplicaNotAvailable = proto.ErrReplicaNotAvailable

	// KafkaErrNoData is a wrapper over kafka.ErrNoData
	KafkaErrNoData = kafka.ErrNoData
)

type kafkaLogger struct {
	subsys string
}

func (l *kafkaLogger) Debug(msg string, args ...interface{}) {
	e := log.NewEntry(log.StandardLogger())

	for i := 0; i < len(args); i += 2 {
		k := fmt.Sprintf("%+v", args[i])
		e = e.WithField(k, args[i+1])
	}

	e.Debugf("[%s] %s", l.subsys, msg)
}

func (l *kafkaLogger) Info(msg string, args ...interface{}) {
	e := log.NewEntry(log.StandardLogger())

	for i := 0; i < len(args); i += 2 {
		k := fmt.Sprintf("%+v", args[i])
		e = e.WithField(k, args[i+1])
	}

	e.Infof("[%s] %s", l.subsys, msg)
}

func (l *kafkaLogger) Warn(msg string, args ...interface{}) {
	e := log.NewEntry(log.StandardLogger())

	for i := 0; i < len(args); i += 2 {
		k := fmt.Sprintf("%+v", args[i])
		e = e.WithField(k, args[i+1])
	}

	e.Warningf("[%s] %s", l.subsys, msg)
}

func (l *kafkaLogger) Error(msg string, args ...interface{}) {
	e := log.NewEntry(log.StandardLogger())

	for i := 0; i < len(args); i += 2 {
		k := fmt.Sprintf("%+v", args[i])
		e = e.WithField(k, args[i+1])
	}

	e.Errorf("[%s] %s", l.subsys, msg)
}

// KhpError is our own errors
type KhpError struct {
	message string
}

func (e KhpError) Error() string {
	return e.message
}

// KafkaClient is batch of brokers
type KafkaClient struct {
	allBrokers    map[int64]*kafka.Broker
	freeBrokers   chan int64
	stopReconnect chan int
}

// NewClient creates new KafkaClient
func NewClient(settings *Config) (*KafkaClient, error) {
	conf := kafka.NewBrokerConf("kafka-http-proxy")

	conf.Logger = &kafkaLogger{
		subsys: "kafka/broker",
	}

	conf.DialTimeout = settings.Broker.DialTimeout.Duration
	conf.LeaderRetryLimit = settings.Broker.LeaderRetryLimit
	conf.LeaderRetryWait = settings.Broker.LeaderRetryWait.Duration

	log.Debug("Gona create broker pool = ", settings.Broker.NumConns)

	client := &KafkaClient{
		allBrokers:    make(map[int64]*kafka.Broker),
		freeBrokers:   make(chan int64, settings.Broker.NumConns),
		stopReconnect: make(chan int, 1),
	}

	brokerID := int64(0)

	for brokerID < settings.Broker.NumConns {
		b, err := kafka.Dial(settings.Kafka.Broker, conf)
		if err != nil {
			_ = client.Close()
			return nil, err
		}

		client.allBrokers[brokerID] = b
		client.freeBrokers <- brokerID
		brokerID++
	}

	if settings.Broker.ReconnectTimeout.Duration > 0 {
		go func() {
			for {
				select {
				case _, ok := <-client.stopReconnect:
					if ok {
						break
					}
				default:
				}

				time.Sleep(settings.Broker.ReconnectTimeout.Duration)

				brokerID, err := client.Broker()
				if err != nil {
					continue
				}

				client.allBrokers[brokerID].Close()

				var b *kafka.Broker
				for {
					b, err = kafka.Dial(settings.Kafka.Broker, conf)
					if err == nil {
						break
					}
					conf.Logger.Error("Unable to reconnect", "brokerID", brokerID, "err", err.Error())
				}

				client.allBrokers[brokerID] = b
				client.freeBrokers <- brokerID

				conf.Logger.Info("Connection was reset by schedule", "brokerID", brokerID)
			}
		}()
	}

	return client, nil
}

// Close closes all brokers.
func (k *KafkaClient) Close() error {
	k.stopReconnect <- 1
	for _, broker := range k.allBrokers {
		broker.Close()
	}
	return nil
}

// Broker returns first availiable broker or error.
func (k *KafkaClient) Broker() (int64, error) {
	select {
	case brokerID, ok := <-k.freeBrokers:
		if ok {
			return brokerID, nil
		}
	default:
	}
	return 0, KhpError{message: "no brokers available"}
}

// NewConsumer creates a new Consumer.
func (k *KafkaClient) NewConsumer(settings *Config, topic string, partitionID int32, offset int64) (*KafkaConsumer, error) {
	var err error

	brokerID, err := k.Broker()
	if err != nil {
		return nil, err
	}

	conf := kafka.NewConsumerConf(topic, partitionID)

	conf.Logger = &kafkaLogger{
		subsys: "kafka/consumer",
	}

	conf.RequestTimeout = settings.Consumer.RequestTimeout.Duration
	conf.RetryLimit = settings.Consumer.RetryLimit
	conf.RetryWait = settings.Consumer.RetryWait.Duration
	conf.RetryErrLimit = settings.Consumer.RetryErrLimit
	conf.RetryErrWait = settings.Consumer.RetryErrWait.Duration
	conf.MinFetchSize = settings.Consumer.MinFetchSize
	conf.MaxFetchSize = settings.Consumer.MaxFetchSize
	conf.StartOffset = offset

	consumer, err := k.allBrokers[brokerID].Consumer(conf)
	if err != nil {
		k.freeBrokers <- brokerID
		return nil, err
	}

	return &KafkaConsumer{
		client:   k,
		brokerID: brokerID,
		consumer: consumer,
		opened:   true,
	}, nil
}

// NewProducer creates a new Producer.
func (k *KafkaClient) NewProducer(settings *Config) (*KafkaProducer, error) {
	brokerID, err := k.Broker()
	if err != nil {
		return nil, err
	}

	conf := kafka.NewProducerConf()

	conf.Logger = &kafkaLogger{
		subsys: "kafka/producer",
	}

	conf.RequestTimeout = settings.Producer.RequestTimeout.Duration
	conf.RetryLimit = settings.Producer.RetryLimit
	conf.RetryWait = settings.Producer.RetryWait.Duration
	conf.RequiredAcks = proto.RequiredAcksAll

	return &KafkaProducer{
		client:   k,
		brokerID: brokerID,
		producer: k.allBrokers[brokerID].Producer(conf),
	}, nil
}

// GetMetadata returns metadata from kafka.
func (k *KafkaClient) GetMetadata() (*KafkaMetadata, error) {
	var err error

	brokerID, err := k.Broker()
	if err != nil {
		return nil, err
	}

	meta := &KafkaMetadata{
		client: k,
	}
	meta.Metadata, err = k.allBrokers[brokerID].Metadata()

	k.freeBrokers <- brokerID

	if err != nil {
		return nil, err
	}
	return meta, nil
}

// KafkaMetadata is a wrapper around metadata response
type KafkaMetadata struct {
	client   *KafkaClient
	Metadata *proto.MetadataResp
}

// Topics returns list of known topics
func (m *KafkaMetadata) Topics() ([]string, error) {
	var topics []string

	for _, topic := range m.Metadata.Topics {
		if topic.Err != nil && topic.Err != proto.ErrLeaderNotAvailable {
			return nil, topic.Err
		}
		topics = append(topics, topic.Name)
	}

	return topics, nil
}

func (m *KafkaMetadata) inTopics(name string) (bool, error) {
	for _, topic := range m.Metadata.Topics {
		if topic.Err != nil {
			return false, topic.Err
		}

		if name == topic.Name {
			return true, nil
		}
	}
	return false, nil
}

type partitionType int

const (
	allPartitions partitionType = iota
	writablePartitions
	maxPartitionIndex
)

func (m *KafkaMetadata) getPartitions(topic string, pType partitionType) ([]int32, error) {
	var partitions []int32

	for _, t := range m.Metadata.Topics {
		if t.Err != nil {
			return nil, t.Err
		}

		if t.Name != topic {
			continue
		}

		for _, p := range t.Partitions {
			if pType == writablePartitions && p.Err == proto.ErrLeaderNotAvailable {
				continue
			}
			partitions = append(partitions, p.ID)
		}
	}

	return partitions, nil
}

// Partitions returns list of partitions.
func (m *KafkaMetadata) Partitions(topic string) ([]int32, error) {
	return m.getPartitions(topic, allPartitions)
}

// WritablePartitions returns list of partitions with a leader.
func (m *KafkaMetadata) WritablePartitions(topic string) ([]int32, error) {
	return m.getPartitions(topic, writablePartitions)
}

// Leader returns the ID of the node which is the leader for partition.
func (m *KafkaMetadata) Leader(topic string, partitionID int32) (int32, error) {
	for _, t := range m.Metadata.Topics {
		if t.Err != nil {
			return -1, t.Err
		}

		if t.Name != topic {
			continue
		}

		for _, p := range t.Partitions {
			if p.ID != partitionID {
				continue
			}
			return p.Leader, nil
		}
	}

	return -1, nil
}

// Replicas returns list of replicas for partition.
func (m *KafkaMetadata) Replicas(topic string, partitionID int32) ([]int32, error) {
	for _, t := range m.Metadata.Topics {
		if t.Err != nil {
			return nil, t.Err
		}

		if t.Name != topic {
			continue
		}

		for _, p := range t.Partitions {
			if p.ID != partitionID {
				continue
			}
			return p.Isrs, nil
		}
	}

	var isr []int32
	return isr, nil
}

// GetOffsetInfo returns newest or oldest offset for partition.
func (m *KafkaMetadata) GetOffsetInfo(topic string, partitionID int32, oType int) (offset int64, err error) {
	brokerID, err := m.client.Broker()
	if err != nil {
		return 0, err
	}
	defer func() {
		m.client.freeBrokers <- brokerID
	}()

	for retry := 0; retry < 2; retry++ {
		switch oType {
		case KafkaOffsetNewest:
			offset, err = m.client.allBrokers[brokerID].OffsetLatest(topic, partitionID)
		case KafkaOffsetOldest:
			offset, err = m.client.allBrokers[brokerID].OffsetEarliest(topic, partitionID)
		}
		if _, ok := err.(*proto.KafkaError); !ok {
			continue
		}
		return
	}
	return
}

// KafkaConsumer is a wrapper around kafka.Consumer.
type KafkaConsumer struct {
	client      *KafkaClient
	brokerID    int64
	consumer    kafka.Consumer
	opened      bool
	ReadTimeout time.Duration
}

// Close frees the connection and returns it to the free pool.
func (c *KafkaConsumer) Close() error {
	if c.opened {
		c.client.freeBrokers <- c.brokerID
		c.opened = false
	}
	return nil
}

// Message returns message from kafka.
func (c *KafkaConsumer) Message() (msg *proto.Message, err error) {
	msgResult := make(chan *proto.Message)
	msgError := make(chan error)
	msgTimeout := make(chan struct{})

	if c.ReadTimeout > 0 {
		timer := time.AfterFunc(c.ReadTimeout, func() { msgTimeout <- struct{}{} })
		defer timer.Stop()
	}

	go func() {
		res, err := c.consumer.Consume()
		if err != nil {
			msgError <- err
			return
		}
		msgResult <- res
	}()

	select {
	case msg = <-msgResult:
	case err = <-msgError:
	case _, ok := <-msgTimeout:
		if ok {
			err = KhpError{message: "Timeout"}
		}
	}
	return
}

// KafkaProducer is a wrapper around kafka.Producer.
type KafkaProducer struct {
	client   *KafkaClient
	brokerID int64
	producer kafka.Producer
}

// Close frees the connection and returns it to the free pool.
func (p *KafkaProducer) Close() error {
	p.client.freeBrokers <- p.brokerID
	return nil
}

// SendMessage sends message in kafka.
func (p *KafkaProducer) SendMessage(topic string, partitionID int32, message []byte) (int64, error) {
	return p.producer.Produce(topic, partitionID, &proto.Message{
		Value: message,
	})
}
