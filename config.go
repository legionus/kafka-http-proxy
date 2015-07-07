/*
* Copyright (C) 2015 Alexey Gladkov <gladkov.alexey@gmail.com>
*
* This file is covered by the GNU General Public License,
* which should be included with kafka-http-proxy as the file COPYING.
 */

package main

import (
	"io"
	"time"
)

// CfgDuration is a Duration wrapper for Config.
type CfgDuration struct {
	time.Duration
}

// UnmarshalText is a wrapper.
func (d *CfgDuration) UnmarshalText(data []byte) (err error) {
	d.Duration, err = time.ParseDuration(string(data))
	return
}

// Config is a main config structure
type Config struct {
	Global struct {
		Address    string
		Logfile    string
		Pidfile    string
		Verbose    bool
		GoMaxProcs int
		MaxConns   int64
	}
	Kafka struct {
		Broker []string
	}
	Metadata struct {
		CacheTimeout CfgDuration
	}
	Broker struct {
		NumConns         int64
		DialTimeout      CfgDuration
		LeaderRetryLimit int
		LeaderRetryWait  CfgDuration
	}
	Producer struct {
		RequestTimeout CfgDuration
		RetryLimit     int
		RetryWait      CfgDuration
	}
	Consumer struct {
		RequestTimeout   CfgDuration
		RetryLimit       int
		RetryWait        CfgDuration
		RetryErrLimit    int
		RetryErrWait     CfgDuration
		MinFetchSize     int32
		MaxFetchSize     int32
		DefaultFetchSize int32
	}

	Logfile io.Writer
}

// SetDefaults applies default values to config structure.
func (c *Config) SetDefaults() {
	c.Global.Verbose = false
	c.Global.GoMaxProcs = 0
	c.Global.MaxConns = 1000000
	c.Global.Logfile = "/var/log/kafka-http-proxy.log"
	c.Global.Pidfile = "/run/kafka-http-proxy.pid"

	c.Metadata.CacheTimeout.Duration = 3 * time.Second

	c.Broker.NumConns = 100
	c.Broker.DialTimeout.Duration = 500 * time.Millisecond
	c.Broker.LeaderRetryLimit = 2
	c.Broker.LeaderRetryWait.Duration = 500 * time.Millisecond

	c.Producer.RequestTimeout.Duration = 5 * time.Second
	c.Producer.RetryLimit = 2
	c.Producer.RetryWait.Duration = 200 * time.Millisecond

	c.Consumer.RequestTimeout.Duration = 50 * time.Millisecond
	c.Consumer.RetryLimit = 2
	c.Consumer.RetryWait.Duration = 50 * time.Millisecond
	c.Consumer.RetryErrLimit = 2
	c.Consumer.RetryErrWait.Duration = 50 * time.Millisecond
	c.Consumer.MinFetchSize = 1
	c.Consumer.MaxFetchSize = 4194304
	c.Consumer.DefaultFetchSize = 524288
}
