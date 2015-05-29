/*
* Copyright (C) 2015 Alexey Gladkov <gladkov.alexey@gmail.com>
*
* This file is covered by the GNU General Public License,
* which should be included with kafka-http-proxy as the file COPYING.
 */

package main

import (
	"time"

	"github.com/Shopify/sarama"
)

type CfgDuration struct {
	time.Duration
}

func (d *CfgDuration) UnmarshalText(data []byte) (err error) {
	d.Duration, err = time.ParseDuration(string(data))
	return
}

type Config struct {
	Global struct {
		Address string
		Logfile string
		Verbose bool
	}
	Kafka struct {
		Broker []string
	}
	Net struct {
		MaxOpenRequests int
		DialTimeout     CfgDuration
		ReadTimeout     CfgDuration
		WriteTimeout    CfgDuration
	}
	Metadata struct {
		RetryMax         int
		RetryBackoff     CfgDuration
		RefreshFrequency CfgDuration
	}
	Producer struct {
		FlushBytes       int
		FlushMessages    int
		FlushMaxMessages int
		FlushFrequency   CfgDuration
	}
	Consumer struct {
		RetryBackoff CfgDuration
		MaxWaitTime  CfgDuration
		FetchMin     int32
		FetchMax     int32
		FetchDefault int32
	}
}

func (c *Config) SetDefaults() {
	c.Global.Verbose = false
	c.Global.Logfile = "/var/log/kafka-http-proxy.log"

	c.Net.MaxOpenRequests = 5
	c.Net.DialTimeout.Duration = 30 * time.Second
	c.Net.ReadTimeout.Duration = 30 * time.Second
	c.Net.WriteTimeout.Duration = 30 * time.Second

	c.Metadata.RetryMax = 3
	c.Metadata.RetryBackoff.Duration = 250 * time.Millisecond
	c.Metadata.RefreshFrequency.Duration = 2 * time.Minute

	c.Producer.FlushBytes = int(sarama.MaxRequestSize)
	c.Producer.FlushFrequency.Duration = 100 * time.Millisecond
	c.Producer.FlushMessages = 1
	c.Producer.FlushMaxMessages = 1

	c.Consumer.RetryBackoff.Duration = 2 * time.Second
	c.Consumer.MaxWaitTime.Duration = 250 * time.Millisecond
	c.Consumer.FetchMin = 1
	c.Consumer.FetchMax = sarama.MaxResponseSize
	c.Consumer.FetchDefault = 32768
}

func (c *Config) KafkaConfig() *sarama.Config {
	k := sarama.NewConfig()

	k.Net.MaxOpenRequests = c.Net.MaxOpenRequests
	k.Net.DialTimeout = c.Net.DialTimeout.Duration
	k.Net.ReadTimeout = c.Net.ReadTimeout.Duration
	k.Net.WriteTimeout = c.Net.WriteTimeout.Duration

	k.Metadata.Retry.Max = c.Metadata.RetryMax
	k.Metadata.Retry.Backoff = c.Metadata.RetryBackoff.Duration
	k.Metadata.RefreshFrequency = c.Metadata.RefreshFrequency.Duration

	// Producer settings
	k.Producer.Partitioner = sarama.NewManualPartitioner
	k.Producer.RequiredAcks = sarama.WaitForAll
	k.Producer.Retry.Max = 10

	k.Producer.Flush.Bytes = c.Producer.FlushBytes
	k.Producer.Flush.Frequency = c.Producer.FlushFrequency.Duration
	k.Producer.Flush.Messages = c.Producer.FlushMessages
	k.Producer.Flush.MaxMessages = c.Producer.FlushMaxMessages

	// Consumer settings
	k.Consumer.Return.Errors = true
	k.Consumer.Retry.Backoff = c.Consumer.RetryBackoff.Duration
	k.Consumer.MaxWaitTime = c.Consumer.MaxWaitTime.Duration
	k.Consumer.Fetch.Min = c.Consumer.FetchMin
	k.Consumer.Fetch.Max = c.Consumer.FetchMax
	k.Consumer.Fetch.Default = c.Consumer.FetchDefault

	return k
}
