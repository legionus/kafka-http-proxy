/*
* Copyright (C) 2015 Alexey Gladkov <gladkov.alexey@gmail.com>
*
* This file is covered by the GNU General Public License,
* which should be included with kafka-http-proxy as the file COPYING.
 */

package main

import (
	"github.com/facebookgo/metrics"

	"time"
	"runtime"
	"syscall"
)

type MetricStats struct {
	ResponsePostTime metrics.Timer
	ResponseGetTime  metrics.Timer
	HTTPStatus       map[int]metrics.Counter
}

func NewMetricStats() *MetricStats {
	m := &MetricStats{
		ResponsePostTime: metrics.NewTimer(),
		ResponseGetTime:  metrics.NewTimer(),
		HTTPStatus:       NewHTTPStatus([]int{200, 400, 404, 405, 416, 500, 502, 503}),
	}

	go func() {
		for {
			m.ResponseGetTime.Tick()
			m.ResponsePostTime.Tick()
			time.Sleep(time.Second)
		}
	}()

	return m
}

type StatTimer struct {
	Min   int64
	Max   int64
	Avg   float64
	Count int64

	Rate1   float64
	Rate5   float64
	Rate15  float64
	RateAvg float64
}

func GetTimerStat(s metrics.Timer) *StatTimer {
	return &StatTimer{
		Min:     s.Min(),
		Max:     s.Max(),
		Avg:     s.Mean(),
		Count:   s.Count(),
		Rate1:   s.Rate1(),
		Rate5:   s.Rate5(),
		Rate15:  s.Rate15(),
		RateAvg: s.RateMean(),
	}
}

type RuntimeStat struct {
	Goroutines      int
	CgoCall         int64
	CPU             int
	GoMaxProcs      int
	UsedDescriptors int
}

func GetRuntimeStat() *RuntimeStat {
	data := &RuntimeStat{
		Goroutines:      runtime.NumGoroutine(),
		CgoCall:         runtime.NumCgoCall(),
		CPU:             runtime.NumCPU(),
		GoMaxProcs:      runtime.GOMAXPROCS(0),
		UsedDescriptors: 0,
	}

	var nofileLimit syscall.Rlimit
	err := syscall.Getrlimit(syscall.RLIMIT_NOFILE, &nofileLimit)
	if err != nil {
		return data
	}
	for i := 0; i < int(nofileLimit.Cur); i++ {
		_, _, errno := syscall.Syscall(syscall.SYS_FCNTL, uintptr(i), syscall.F_GETFD, 0)
		if errno == 0 {
			data.UsedDescriptors++
		}
	}
	return data
}

func NewHTTPStatus(codes []int) map[int]metrics.Counter {
	HTTPStatus := make(map[int]metrics.Counter)

	for _, code := range codes {
		HTTPStatus[code] = metrics.NewCounter()
	}
	return HTTPStatus
}
