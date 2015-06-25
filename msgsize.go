/*
* Copyright (C) 2015 Alexey Gladkov <gladkov.alexey@gmail.com>
*
* This file is covered by the GNU General Public License,
* which should be included with kafka-http-proxy as the file COPYING.
 */

package main

import (
	"sync"
)

type TopicMessageSize struct {
	sync.RWMutex
	Topics map[string]int32
}

func NewTopicMessageSize() TopicMessageSize {
	return TopicMessageSize{
		Topics: make(map[string]int32, 0),
	}
}

func (c *TopicMessageSize) Get(topic string, defval int32) int32 {
	c.Lock()
	defer c.Unlock()

	if val, ok := c.Topics[topic]; ok {
		return val
	}
	return defval
}

func (c *TopicMessageSize) Set(topic string, size int32) {
	c.Lock()
	defer c.Unlock()

	c.Topics[topic] = size
}

func (c *TopicMessageSize) Put(topic string, size int32) {
	c.Lock()
	defer c.Unlock()

	if val, ok := c.Topics[topic]; ok {
		c.Topics[topic] = int32((val + size) / 2)
		return
	}

	c.Topics[topic] = size
}
