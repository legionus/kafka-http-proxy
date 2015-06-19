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
)

var (
	KafkaOffsetNewest = kafka.StartOffsetNewest
	KafkaOffsetOldest = kafka.StartOffsetOldest

	KafkaErrReplicaNotAvailable = proto.ErrReplicaNotAvailable
	KafkaErrNoData              = kafka.ErrNoData
)

func KafkaConfig(settings Config) (kafka.BrokerConf, error) {
	k := kafka.NewBrokerConf("kafka-http-proxy")

	k.DialTimeout = settings.Broker.DialTimeout.Duration
	k.LeaderRetryLimit = settings.Broker.LeaderRetryLimit
	k.LeaderRetryWait = settings.Broker.LeaderRetryWait.Duration

	return k, nil
}

type KafkaClient struct {
	Brokers *kafka.Broker
}

func NewClient(addrs []string, conf kafka.BrokerConf) (*KafkaClient, error) {
	var err error

	client := &KafkaClient{}

	client.Brokers, err = kafka.Dial(addrs, conf)
	if err != nil {
		return nil, err
	}

	return client, nil
}

func (k *KafkaClient) Close() error {
	k.Brokers.Close()
	return nil
}

func (k *KafkaClient) NewConsumer(settings Config, topic string, partitionID int32, offset int64) (*KafkaConsumer, error) {
	var err error

	conf := kafka.NewConsumerConf(topic, partitionID)

	conf.RequestTimeout = settings.Consumer.RequestTimeout.Duration
	conf.RetryLimit = settings.Consumer.RetryLimit
	conf.RetryWait = settings.Consumer.RetryWait.Duration
	conf.RetryErrLimit = settings.Consumer.RetryErrLimit
	conf.RetryErrWait = settings.Consumer.RetryErrWait.Duration
	conf.MinFetchSize = settings.Consumer.MinFetchSize
	conf.MaxFetchSize = settings.Consumer.MaxFetchSize

	conf.StartOffset = offset

	c := &KafkaConsumer{}

	c.consumer, err = k.Brokers.Consumer(conf)
	if err != nil {
		return nil, err
	}

	return c, nil
}

func (k *KafkaClient) NewProducer(settings Config) (*KafkaProducer, error) {
	conf := kafka.NewProducerConf()

	conf.RequestTimeout = settings.Producer.RequestTimeout.Duration
	conf.RetryLimit = settings.Producer.RetryLimit
	conf.RetryWait = settings.Producer.RetryWait.Duration
	conf.RequiredAcks = proto.RequiredAcksAll

	p := &KafkaProducer{
		producer: k.Brokers.Producer(conf),
	}

	return p, nil
}

func (k *KafkaClient) GetMetadata() (*KafkaMetadata, error) {
	var err error

	meta := &KafkaMetadata{
		client: k,
	}

	meta.Metadata, err = k.Brokers.Metadata()
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
	switch oType {
	case KafkaOffsetNewest:
		offset, err = m.client.Brokers.OffsetLatest(topic, partitionID)
	case KafkaOffsetOldest:
		offset, err = m.client.Brokers.OffsetEarliest(topic, partitionID)
	}
	return
}

type KafkaConsumer struct {
	consumer kafka.Consumer
}

func (c *KafkaConsumer) Close() error {
	return nil
}

func (c *KafkaConsumer) Message() (*proto.Message, error) {
	return c.consumer.Consume()
}

type KafkaProducer struct {
	producer kafka.Producer
}

func (p *KafkaProducer) Close() error {
	return nil
}

func (p *KafkaProducer) SendMessage(topic string, partitionID int32, message []byte) (int64, error) {
	return p.producer.Produce(topic, partitionID, &proto.Message{
		Value: message,
	})
}
