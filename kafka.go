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
	"sync"
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

const (
	_                     = iota
	KhpErrorNoBrokers int = 1
	KhpErrorReadTimeout
	KhpErrorWriteTimeout
	KhpErrorConsumerClosed
	KhpErrorProducerClosed
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
	Errno   int
	message string
}

func (e KhpError) Error() string {
	return e.message
}

// KafkaClient is batch of brokers
type KafkaClient struct {
	CacheTimeout time.Duration
	ReadTimeout  time.Duration

	allBrokers    map[int64]*kafka.Broker
	deadBrokers   chan int64
	freeBrokers   chan int64
	stopReconnect chan struct{}

	cache struct {
		sync.RWMutex

		lastMetadata       *KafkaMetadata
		lastUpdateMetadata int64
	}
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
		CacheTimeout:  settings.Metadata.CacheTimeout.Duration,
		ReadTimeout:   settings.Broker.ReadTimeout.Duration,
		allBrokers:    make(map[int64]*kafka.Broker),
		deadBrokers:   make(chan int64, settings.Broker.NumConns),
		freeBrokers:   make(chan int64, settings.Broker.NumConns),
		stopReconnect: make(chan struct{}),
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

	if settings.Metadata.CacheTimeout.Duration > 0 {
		go func() {
			for {
				select {
				case <-time.After(settings.Metadata.CacheTimeout.Duration):
				case <-client.stopReconnect:
					return
				}

				meta, err := client.GetMetadata()
				if err != nil {
					continue
				}

				client.cache.Lock()
				client.cache.lastMetadata = meta
				client.cache.lastUpdateMetadata = time.Now().UnixNano()
				client.cache.Unlock()
			}
		}()
	}

	if settings.Broker.ReconnectTimeout.Duration > 0 {
		go func() {
			var id int64
			var goErr error

			for {
				select {
				case id = <-client.deadBrokers:
				case <-time.After(settings.Broker.ReconnectTimeout.Duration):
					id, goErr = client.Broker()
					if goErr != nil {
						continue
					}
				case <-client.stopReconnect:
					return
				}

				client.allBrokers[id].Close()

				for {
					b, goErr := kafka.Dial(settings.Kafka.Broker, conf)
					if goErr == nil {
						client.allBrokers[id] = b
						client.freeBrokers <- id
						break
					}
					conf.Logger.Error("Unable to reconnect", "brokerID", id, "err", goErr.Error())
				}
				conf.Logger.Info("Connection was reset by schedule", "brokerID", id)
			}
		}()
	}

	return client, nil
}

// Close closes all brokers.
func (k *KafkaClient) Close() error {
	close(k.stopReconnect)
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
	return 0, KhpError{
		Errno:   KhpErrorNoBrokers,
		message: "no brokers available",
	}
}

// GetOffsets returns newest and oldest offsets for partition.
func (k *KafkaClient) GetOffsets(topic string, partitionID int32) (int64, int64, error) {
	brokerID, err := k.Broker()
	if err != nil {
		return 0, 0, err
	}

	type offsetInfo struct {
		result  int64
		fetcher func(string, int32) (int64, error)
	}

	offsets := []offsetInfo{
		offsetInfo{0, k.allBrokers[brokerID].OffsetLatest},
		offsetInfo{0, k.allBrokers[brokerID].OffsetEarliest},
	}

	results := make(chan error, 2)
	timeout := make(chan struct{})

	if k.ReadTimeout > 0 {
		timer := time.AfterFunc(k.ReadTimeout, func() { close(timeout) })
		defer timer.Stop()
	}

	for i := range offsets {
		go func(i int) {
			var goErr error

			for retry := 0; retry < 2; retry++ {
				select {
				case <-timeout:
					return
				default:
				}

				offsets[i].result, goErr = offsets[i].fetcher(topic, partitionID)

				if goErr == nil {
					break
				}

				if _, ok := goErr.(*proto.KafkaError); ok {
					break
				}
			}
			results <- goErr
		}(i)
	}

	for _ = range offsets {
		select {
		case err = <-results:
			if err != nil {
				k.freeBrokers <- brokerID
				break
			}
		case <-timeout:
			k.deadBrokers <- brokerID
			err = KhpError{
				Errno:   KhpErrorReadTimeout,
				message: "Read timeout",
			}
			break
		}
	}

	return offsets[0].result, offsets[1].result, err
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
		client:       k,
		brokerID:     brokerID,
		producer:     k.allBrokers[brokerID].Producer(conf),
		opened:       true,
		WriteTimeout: settings.Producer.WriteTimeout.Duration,
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

// FetchMetadata returns metadata from kafka but use internal cache.
func (k *KafkaClient) FetchMetadata() (*KafkaMetadata, error) {
	k.cache.RLock()
	defer k.cache.RUnlock()

	if k.CacheTimeout > 0 && k.cache.lastUpdateMetadata > 0 {
		period := time.Now().UnixNano() - k.cache.lastUpdateMetadata

		if period < 0 {
			period = -period
		}

		if period < int64(k.CacheTimeout) {
			return k.cache.lastMetadata, nil
		}
	}

	return k.GetMetadata()
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

// Corrupt marks the connection as a broken.
func (c *KafkaConsumer) Corrupt() {
	if !c.opened {
		return
	}
	c.client.deadBrokers <- c.brokerID
	c.opened = false
}

// Message returns message from kafka.
func (c *KafkaConsumer) Message() (msg *proto.Message, err error) {
	if !c.opened {
		err = KhpError{
			Errno:   KhpErrorConsumerClosed,
			message: "Read from closed consumer",
		}
		return
	}

	result := make(chan struct{})
	timeout := make(chan struct{})

	if c.ReadTimeout > 0 {
		timer := time.AfterFunc(c.ReadTimeout, func() { close(timeout) })
		defer timer.Stop()
	}

	var kafkaMsg *proto.Message
	var kafkaErr error

	go func() {
		kafkaMsg, kafkaErr = c.consumer.Consume()
		close(result)
	}()

	select {
	case <-result:
		msg, err = kafkaMsg, kafkaErr
	case <-timeout:
		c.Corrupt()
		err = KhpError{
			Errno:   KhpErrorReadTimeout,
			message: "Read timeout",
		}
	}
	return
}

// KafkaProducer is a wrapper around kafka.Producer.
type KafkaProducer struct {
	client       *KafkaClient
	brokerID     int64
	producer     kafka.Producer
	opened       bool
	WriteTimeout time.Duration
}

// Close frees the connection and returns it to the free pool.
func (p *KafkaProducer) Close() error {
	if p.opened {
		p.client.freeBrokers <- p.brokerID
		p.opened = false
	}
	return nil
}

// Corrupt marks the connection as a broken.
func (p *KafkaProducer) Corrupt() {
	if !p.opened {
		return
	}
	p.client.deadBrokers <- p.brokerID
	p.opened = false
}

// SendMessage sends message in kafka.
func (p *KafkaProducer) SendMessage(topic string, partitionID int32, message []byte) (offset int64, err error) {
	if !p.opened {
		err = KhpError{
			Errno:   KhpErrorProducerClosed,
			message: "Write to closed producer",
		}
		return
	}

	result := make(chan struct{})
	timeout := make(chan struct{})

	if p.WriteTimeout > 0 {
		timer := time.AfterFunc(p.WriteTimeout, func() { close(timeout) })
		defer timer.Stop()
	}

	var kafkaOffset int64
	var kafkaErr error

	go func() {
		kafkaOffset, kafkaErr = p.producer.Produce(topic, partitionID, &proto.Message{
			Value: message,
		})
		close(result)
	}()

	select {
	case <-result:
		offset, err = kafkaOffset, kafkaErr
	case <-timeout:
		p.Corrupt()
		err = KhpError{
			Errno:   KhpErrorWriteTimeout,
			message: "Write timeout",
		}
	}
	return
}
