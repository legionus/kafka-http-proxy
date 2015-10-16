package main

import (
	//	"fmt"
	//	"net"
	//	"strings"
	"testing"
	"time"

	"github.com/optiopay/kafka/proto"

	log "github.com/Sirupsen/logrus"
)

func setLogFormat(cfg *Config) {
	log.SetFormatter(&log.TextFormatter{
		FullTimestamp:    cfg.Logging.FullTimestamp,
		DisableTimestamp: cfg.Logging.DisableTimestamp,
		DisableColors:    cfg.Logging.DisableColors,
		DisableSorting:   cfg.Logging.DisableSorting,
	})
}

func TestNewClient(t *testing.T) {
	srv := NewKafkaServer()
	srv.Start()
	defer srv.Close()

	cfg := &Config{}
	cfg.SetDefaults()
	cfg.Kafka.Broker = []string{srv.Address()}
	cfg.Global.Verbose = true
	cfg.Broker.NumConns = 5

	//log.SetLevel(log.DebugLevel)
	setLogFormat(cfg)

	kafkaClient, err := NewClient(cfg)
	if err != nil {
		t.Fatalf("Unable to make client: %s", err.Error())
	}
	kafkaClient.Close()
}

func TestGetBroker(t *testing.T) {
	srv := NewKafkaServer()
	srv.Start()
	defer srv.Close()

	cfg := &Config{}
	cfg.SetDefaults()
	cfg.Kafka.Broker = []string{srv.Address()}
	cfg.Global.Verbose = true
	cfg.Broker.NumConns = 2

	//log.SetLevel(log.DebugLevel)
	setLogFormat(cfg)

	kafkaClient, err := NewClient(cfg)
	if err != nil {
		t.Fatalf("unable to make client: %s", err)
	}

	if _, err := kafkaClient.getBroker(); err != nil {
		t.Fatalf("unexpected error: %s", err)
	}
	if _, err := kafkaClient.getBroker(); err != nil {
		t.Fatalf("unexpected error: %s", err)
	}

	_, err = kafkaClient.getBroker()
	if err == nil {
		t.Fatalf("got broker, but shouldn't have")
	}
	if _, ok := err.(KhpError); !ok {
		t.Fatalf("got wrong error: %s", err)
	}

	kafkaClient.Close()
}

func TestConsumer(t *testing.T) {
	srv := NewKafkaServer()
	srv.Start()
	defer srv.Close()

	srv.Handle(MetadataRequest, func(request Serializable) Serializable {
		req := request.(*proto.MetadataReq)
		host, port := srv.HostPort()
		return &proto.MetadataResp{
			CorrelationID: req.CorrelationID,
			Brokers: []proto.MetadataRespBroker{
				{NodeID: 1, Host: host, Port: int32(port)},
			},
			Topics: []proto.MetadataRespTopic{
				{
					Name: "test",
					Partitions: []proto.MetadataRespPartition{
						{
							ID:       413,
							Leader:   1,
							Replicas: []int32{1},
							Isrs:     []int32{1},
						},
					},
				},
			},
		}
	})
	fetchCallCount := 0
	srv.Handle(FetchRequest, func(request Serializable) Serializable {
		req := request.(*proto.FetchReq)
		fetchCallCount++
		if fetchCallCount < 1 {
			return &proto.FetchResp{
				CorrelationID: req.CorrelationID,
				Topics: []proto.FetchRespTopic{
					{
						Name: "test",
						Partitions: []proto.FetchRespPartition{
							{
								ID:        413,
								TipOffset: 0,
								Messages:  []*proto.Message{},
							},
						},
					},
				},
			}
		}

		messages := []*proto.Message{
			{Offset: 3, Key: []byte("1"), Value: []byte("first")},
			{Offset: 4, Key: []byte("2"), Value: []byte("second")},
			{Offset: 5, Key: []byte("3"), Value: []byte("three")},
		}

		return &proto.FetchResp{
			CorrelationID: req.CorrelationID,
			Topics: []proto.FetchRespTopic{
				{
					Name: "test",
					Partitions: []proto.FetchRespPartition{
						{
							ID:        413,
							TipOffset: 2,
							Messages:  messages,
						},
					},
				},
			},
		}
	})

	cfg := &Config{}
	cfg.SetDefaults()
	cfg.Kafka.Broker = []string{srv.Address()}
	cfg.Global.Verbose = true
	cfg.Broker.NumConns = 2

	//log.SetLevel(log.DebugLevel)
	setLogFormat(cfg)

	kafkaClient, err := NewClient(cfg)
	if err != nil {
		t.Fatalf("unable to make client: %s", err)
	}

	consumer, err := kafkaClient.NewConsumer(cfg, "test", 413, 0)
	if err != nil {
		t.Fatalf("unable to make consumer: %s", err)
	}

	msg, err := consumer.Message()
	if err != nil {
		t.Fatalf("expected no errors, got %s", err)
	}

	if string(msg.Value) != "first" || string(msg.Key) != "1" || msg.Offset != 3 {
		t.Fatalf("expected different message than %#v", msg)
	}

	consumer.Close()
	kafkaClient.Close()
}

func TestConsumerTimeout(t *testing.T) {
	srv := NewKafkaServer()
	srv.Start()
	defer srv.Close()

	srv.Handle(MetadataRequest, func(request Serializable) Serializable {
		req := request.(*proto.MetadataReq)
		host, port := srv.HostPort()
		return &proto.MetadataResp{
			CorrelationID: req.CorrelationID,
			Brokers: []proto.MetadataRespBroker{
				{NodeID: 1, Host: host, Port: int32(port)},
			},
			Topics: []proto.MetadataRespTopic{
				{
					Name: "test",
					Partitions: []proto.MetadataRespPartition{
						{
							ID:       413,
							Leader:   1,
							Replicas: []int32{1},
							Isrs:     []int32{1},
						},
					},
				},
			},
		}
	})
	fetchCallCount := 0
	srv.Handle(FetchRequest, func(request Serializable) Serializable {
		req := request.(*proto.FetchReq)
		fetchCallCount++
		if fetchCallCount < 1 {
			return &proto.FetchResp{
				CorrelationID: req.CorrelationID,
				Topics: []proto.FetchRespTopic{
					{
						Name: "test",
						Partitions: []proto.FetchRespPartition{
							{
								ID:        413,
								TipOffset: 0,
								Messages:  []*proto.Message{},
							},
						},
					},
				},
			}
		}

		return nil
	})

	cfg := &Config{}
	cfg.SetDefaults()
	cfg.Kafka.Broker = []string{srv.Address()}
	cfg.Global.Verbose = true
	cfg.Broker.NumConns = 2

	//log.SetLevel(log.DebugLevel)
	setLogFormat(cfg)

	kafkaClient, err := NewClient(cfg)
	if err != nil {
		t.Fatalf("unable to make client: %s", err)
	}

	consumer, err := kafkaClient.NewConsumer(cfg, "test", 413, 0)
	if err != nil {
		t.Fatalf("unable to make consumer: %s", err)
	}
	consumer.ReadTimeout = time.Second

	msg, err := consumer.Message()
	if _, ok := err.(KhpError); !ok {
		t.Fatalf("expected no errors, got %s", err)
	}

	if err.(KhpError).Errno != KhpErrorReadTimeout {
		t.Fatalf("expected KhpErrorReadTimeout, got %s", err)
	}

	if msg != nil {
		t.Fatalf("unexpected result: %#v", msg)
	}

	consumer.Close()
	kafkaClient.Close()
}
