package main

import (
//	"fmt"
	"testing"
)

func TestCMAAdd(t *testing.T) {
	d := NewCMA(5)

	if d.Mean() != 0 {
		t.Fatal("expected value is 0, got", d.Mean())
	}

	d.Add(1, 2, 3, 4, 5, 6, 7, 8, 9, 10)

	if d.Mean() != float64(8) {
		t.Fatal("expected value is 8, got", d.Mean())
	}
}

func BenchmarkCMAAdd(b *testing.B) {
	d := NewCMA(1000)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		d.Add(42)
	}
}

func BenchmarkCMAValue100(b *testing.B) {
	d := populatedCMA(100)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		d.Mean()
	}
}

func BenchmarkCMAValue1000(b *testing.B) {
	d := populatedCMA(1000)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		d.Mean()
	}
}

func BenchmarkCMAValue10000(b *testing.B) {
	d := populatedCMA(10000)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		d.Mean()
	}
}

func BenchmarkCMAValue1000000(b *testing.B) {
	d := populatedCMA(1000000)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		d.Mean()
	}
}

func populatedCMA(n int) *CMA {
	d := NewCMA(n)
	samples := make([]int64, n)
	for i := 0; i < n; i++ {
		samples[i] = int64(i)
	}
	d.Add(samples...)
	return d
}
