/*
* Copyright (C) 2015 Alexey Gladkov <gladkov.alexey@gmail.com>
*
* This file is covered by the GNU General Public License,
* which should be included with kafka-http-proxy as the file COPYING.
 */

package main

import (
	"github.com/facebookgo/metrics"
)

// TopicMessageSize contains map of topics and their metrics.
type TopicMessageSize struct {
	Topics map[string]metrics.Histogram
}

// NewTopicMessageSize creates a new metric.
func NewTopicMessageSize() *TopicMessageSize {
	c := &TopicMessageSize{
		Topics: make(map[string]metrics.Histogram),
	}
	return c
}

// Get returns value by topic name.
func (c *TopicMessageSize) Get(topic string, defval int32) int32 {
	if val, ok := c.Topics[topic]; ok {
		ret := int32(val.Percentile(0.75))
		if ret < 0 {
			ret = defval
		}
		return ret
	}
	return defval
}

// Put adds another raw value to metric.
func (c *TopicMessageSize) Put(topic string, val int32) {
	if _, ok := c.Topics[topic]; !ok {
		c.Topics[topic] = metrics.NewHistogram(metrics.NewUniformSample(10000))
	}
	if val > 0 {
		c.Topics[topic].Update(int64(val))
	}
}
