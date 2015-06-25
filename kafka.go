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

	"fmt"
	"log"
)

var (
	KafkaOffsetNewest = kafka.StartOffsetNewest
	KafkaOffsetOldest = kafka.StartOffsetOldest

	KafkaErrReplicaNotAvailable = proto.ErrReplicaNotAvailable
	KafkaErrNoData              = kafka.ErrNoData
)

type KafkaClient struct {
	numBrokers int64
	Brokers    chan *kafka.Broker
}

func NewClient(settings Config) (*KafkaClient, error) {
	conf := kafka.NewBrokerConf("kafka-http-proxy")

	conf.Log = log.New(settings.Logfile, "[kafka/broker] ", log.LstdFlags)
	conf.DialTimeout = settings.Broker.DialTimeout.Duration
	conf.LeaderRetryLimit = settings.Broker.LeaderRetryLimit
	conf.LeaderRetryWait = settings.Broker.LeaderRetryWait.Duration

	if settings.Global.Verbose {
		log.Println("Gona create broker pool = ", settings.Broker.NumConns)
	}

	client := &KafkaClient{
		numBrokers: 0,
		Brokers:    make(chan *kafka.Broker, settings.Broker.NumConns),
	}

	for client.numBrokers < settings.Broker.NumConns {
		b, err := kafka.Dial(settings.Kafka.Broker, conf)
		if err != nil {
			client.Close()
			return nil, err
		}

		client.Brokers <- b
		client.numBrokers++
	}

	return client, nil
}

func (k *KafkaClient) Close() error {
	i := int64(0)
	for i < k.numBrokers {
		broker := <-k.Brokers
		broker.Close()
		i++
	}
	return nil
}

func (k *KafkaClient) Broker() (*kafka.Broker, error) {
	select {
	case broker, ok := <-k.Brokers:
		if ok {
			return broker, nil
		}
	default:
	}
	return nil, fmt.Errorf("no brokers available")
}

func (k *KafkaClient) NewConsumer(settings Config, topic string, partitionID int32, offset int64) (*KafkaConsumer, error) {
	var err error

	broker, err := k.Broker()
	if err != nil {
		return nil, err
	}

	conf := kafka.NewConsumerConf(topic, partitionID)

	conf.Log = log.New(settings.Logfile, "[kafka/consumer] ", log.LstdFlags)
	conf.RequestTimeout = settings.Consumer.RequestTimeout.Duration
	conf.RetryLimit = settings.Consumer.RetryLimit
	conf.RetryWait = settings.Consumer.RetryWait.Duration
	conf.RetryErrLimit = settings.Consumer.RetryErrLimit
	conf.RetryErrWait = settings.Consumer.RetryErrWait.Duration
	conf.MinFetchSize = settings.Consumer.MinFetchSize
	conf.MaxFetchSize = settings.Consumer.MaxFetchSize
	conf.StartOffset = offset

	consumer, err := broker.Consumer(conf)
	if err != nil {
		k.Brokers <- broker
		return nil, err
	}

	return &KafkaConsumer{
		client:   k,
		broker:   broker,
		consumer: consumer,
		opened:   true,
	}, nil
}

func (k *KafkaClient) NewProducer(settings Config) (*KafkaProducer, error) {
	broker, err := k.Broker()
	if err != nil {
		return nil, err
	}

	conf := kafka.NewProducerConf()

	conf.Log = log.New(settings.Logfile, "[kafka/producer] ", log.LstdFlags)
	conf.RequestTimeout = settings.Producer.RequestTimeout.Duration
	conf.RetryLimit = settings.Producer.RetryLimit
	conf.RetryWait = settings.Producer.RetryWait.Duration
	conf.RequiredAcks = proto.RequiredAcksAll

	return &KafkaProducer{
		client:   k,
		broker:   broker,
		producer: broker.Producer(conf),
	}, nil
}

func (k *KafkaClient) GetMetadata() (*KafkaMetadata, error) {
	var err error

	broker, err := k.Broker()
	if err != nil {
		return nil, err
	}

	meta := &KafkaMetadata{
		client: k,
	}
	meta.Metadata, err = broker.Metadata()

	k.Brokers <- broker

	if err != nil {
		return nil, err
	}
	return meta, nil
}

type KafkaMetadata struct {
	client   *KafkaClient
	Metadata *proto.MetadataResp
}

func (m *KafkaMetadata) Topics() ([]string, error) {
	topics := make([]string, 0)

	for _, topic := range m.Metadata.Topics {
		if topic.Err != nil && topic.Err != proto.ErrLeaderNotAvailable {
			return nil, topic.Err
		}
		topics = append(topics, topic.Name)
	}

	return topics, nil
}

type partitionType int

const (
	allPartitions partitionType = iota
	writablePartitions
	maxPartitionIndex
)

func (m *KafkaMetadata) getPartitions(topic string, pType partitionType) ([]int32, error) {
	partitions := make([]int32, 0)

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

func (m *KafkaMetadata) Partitions(topic string) ([]int32, error) {
	return m.getPartitions(topic, allPartitions)
}

func (m *KafkaMetadata) WritablePartitions(topic string) ([]int32, error) {
	return m.getPartitions(topic, writablePartitions)
}

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

	isr := make([]int32, 0)
	return isr, nil
}

func (m *KafkaMetadata) GetOffsetInfo(topic string, partitionID int32, oType int) (offset int64, err error) {
	broker, err := m.client.Broker()
	if err != nil {
		return 0, err
	}
	defer func() {
		m.client.Brokers <- broker
	}()

	switch oType {
	case KafkaOffsetNewest:
		offset, err = broker.OffsetLatest(topic, partitionID)
	case KafkaOffsetOldest:
		offset, err = broker.OffsetEarliest(topic, partitionID)
	}
	return
}

type KafkaConsumer struct {
	client   *KafkaClient
	broker   *kafka.Broker
	consumer kafka.Consumer
	opened   bool
}

func (c *KafkaConsumer) Close() error {
	if c.opened {
		c.client.Brokers <- c.broker
		c.opened = false
	}
	return nil
}

func (c *KafkaConsumer) Message() (*proto.Message, error) {
	return c.consumer.Consume()
}

type KafkaProducer struct {
	client   *KafkaClient
	broker   *kafka.Broker
	producer kafka.Producer
}

func (p *KafkaProducer) Close() error {
	p.client.Brokers <- p.broker
	return nil
}

func (p *KafkaProducer) SendMessage(topic string, partitionID int32, message []byte) (int64, error) {
	return p.producer.Produce(topic, partitionID, &proto.Message{
		Value: message,
	})
}
