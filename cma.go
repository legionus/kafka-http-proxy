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

// CMA is cumulative moving average.
type CMA struct {
	sync.RWMutex

	invalidMinMax bool
	max           int64
	min           int64
	index         int
	size          int
	sum           int64
	values        []int64
}

// NewCMA returns initialized CMA with given size.
func NewCMA(size int) (res *CMA) {
	res = &CMA{
		invalidMinMax: true,
		min:           0,
		max:           0,
		index:         0,
		sum:           0,
		size:          size,
		values:        make([]int64, 0, size),
	}
	return
}

// Add adds given values to CMA.
func (d *CMA) Add(values ...int64) {
	d.Lock()
	defer d.Unlock()

	for _, v := range values {
		if len(d.values) < d.size {
			d.values = append(d.values, v)
			d.sum += v
			d.index++
			continue
		}

		d.index = d.index % d.size

		if !d.invalidMinMax {
			d.invalidMinMax = (d.min == d.values[d.index] || d.max == d.values[d.index])
		}

		d.sum -= d.values[d.index]
		d.sum += v
		d.values[d.index] = v
		d.index++
	}
}

func (d *CMA) dec(numItems int) { // FIXME
	d.Lock()
	defer d.Unlock()

	if len(d.values) == 0 {
		return
	}

	if len(d.values) <= numItems {
		d.values = d.values[:0]
		d.index = 0
		d.sum = 0
		d.invalidMinMax = true
		return
	}

	for i := 0; i < numItems; i++ {
		d.sum -= d.values[i]
	}

	d.values = d.values[numItems:]
	d.index = len(d.values)
	d.invalidMinMax = true
}

// Mean returns average of current CMA.
func (d *CMA) Mean() float64 {
	d.RLock()
	defer d.RUnlock()

	if len(d.values) == 0 {
		return 0
	}

	return float64(d.sum / int64(len(d.values)))
}

// Count returns current length.
func (d *CMA) Count() int {
	d.RLock()
	defer d.RUnlock()

	return len(d.values)
}

// MinMax returns minimum and maximum of CMA.
func (d *CMA) MinMax() (int64, int64) {
	d.Lock()
	defer d.Unlock()

	if len(d.values) == 0 {
		return 0, 0
	}

	if d.invalidMinMax {
		d.min, d.max = d.updateMinMax()
		d.invalidMinMax = false
	}

	return d.min, d.max
}

func (d *CMA) updateMinMax() (min int64, max int64) {
	i := 0

	if len(d.values)%2 == 0 {
		if d.values[0] < d.values[1] {
			min = d.values[0]
			max = d.values[1]
		} else {
			min = d.values[0]
			max = d.values[1]
		}
		i = 2
	} else {
			min = d.values[0]
			max = d.values[0]
		i = 1
	}

	for ; i < len(d.values); i += 2 {
		if d.values[i] > d.values[i+1] {
			if d.values[i] > max {
				max = d.values[i]
			}
			if d.values[i+1] < min {
				min = d.values[i+1]
			}
		} else {
			if d.values[i+1] > max {
				max = d.values[i]
			}
			if d.values[i] < min {
				min = d.values[i+1]
			}
		}
	}

	return
}
