/*
* Copyright (C) 2015 Alexey Gladkov <gladkov.alexey@gmail.com>
*
* This file is covered by the GNU General Public License,
* which should be included with kafka-http-proxy as the file COPYING.
 */

package main

import (
	"github.com/facebookgo/metrics"

	"runtime"
	"syscall"
	"time"
)

// ResponseTimer is wrapper around a decaying simple moving average.
type ResponseTimer struct {
	Size int
	MA   *CMA
}

// CaptureTimer is helper to update ResponseTimer.
type CaptureTimer struct {
	timer ResponseTimer
	start time.Time
}

// Stop writes the elapsed time.
func (c *CaptureTimer) Stop() {
	n := int64(time.Since(c.start) / time.Millisecond)
	c.timer.Add(n)
}

// NewResponseTimer creates new ResponseTimer.
func NewResponseTimer(size int) *ResponseTimer {
	return &ResponseTimer{
		Size: size,
		MA:   NewCMA(size),
	}
}

// Start captures the current time.
func (t ResponseTimer) Start() *CaptureTimer {
	return &CaptureTimer{
		timer: t,
		start: time.Now(),
	}
}

// Add appends new value to set.
func (t ResponseTimer) Add(n int64) {
	t.MA.Add(n)
}

// SnapshotTimer is a snapshot of the ResponseTimer values.
type SnapshotTimer struct {
	Count uint64
	Max   int64
	Min   int64
	Avg   float64
	Size  int
}

// GetSnapshot creates a snapshot of the ResponseTimer values.
func (t ResponseTimer) GetSnapshot() (res *SnapshotTimer) {
	res = &SnapshotTimer{}
	res.Count = t.MA.Count()
	res.Size = t.MA.Size()
	res.Avg = t.MA.Mean()
	res.Min, res.Max = t.MA.MinMax()
	return
}

// MetricStats contains statistics about HTTP responses.
type MetricStats struct {
	HTTPStatus       map[int]metrics.Counter
	HTTPResponseTime map[string]*ResponseTimer
}

// NewMetricStats creates new MetricStats object.
func NewMetricStats() *MetricStats {
	return &MetricStats{
		HTTPStatus:       NewHTTPStatus([]int{200, 400, 404, 405, 416, 500, 502, 503}),
		HTTPResponseTime: NewTimings(600, []string{"GET", "POST", "GetTopicList", "GetTopicInfo", "GetPartitionInfo"}),
	}
}

// RuntimeStat contains runtime statistic.
type RuntimeStat struct {
	Goroutines      int
	CgoCall         int64
	CPU             int
	GoMaxProcs      int
	UsedDescriptors int
}

// GetRuntimeStat creates new RuntimeStat object.
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

// NewHTTPStatus creates object for HTTP status statistic.
func NewHTTPStatus(codes []int) map[int]metrics.Counter {
	HTTPStatus := make(map[int]metrics.Counter)

	for _, code := range codes {
		HTTPStatus[code] = metrics.NewCounter()
	}
	return HTTPStatus
}

func NewTimings(size int, names []string) map[string]*ResponseTimer {
	res := make(map[string]*ResponseTimer)

	for _, name := range names {
		res[name] = NewResponseTimer(size)
	}

	return res
}
