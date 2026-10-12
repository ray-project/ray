// Copyright 2026 The Ray Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//  http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// Package metric exposes a public API for reporting custom metrics to Ray.
//
// The package only depends on the Backend interface; the concrete backend is
// injected from outside (go/internal or tests) via SetBackend. This keeps the
// public package decoupled from the internal cgo bridge (DIP).
package metric

import (
	"errors"
	"sync"
)

// Backend is the sink for registered metrics. Implementations may be cgo-backed
// (CgoMetricBackend) or test doubles.
type Backend interface {
	RegisterTagKey(key string) error
	RegisterCount(name, desc, unit string, tagKeys []string) (int64, error)
	RegisterGauge(name, desc, unit string, tagKeys []string) (int64, error)
	RegisterSum(name, desc, unit string, tagKeys []string) (int64, error)
	RegisterHistogram(name, desc, unit string, boundaries []float64, tagKeys []string) (int64, error)
	Record(handle int64, value float64, tagKeys, tagValues []string) error
	Unregister(handle int64) error
}

var (
	mu      sync.RWMutex
	backend Backend
)

// SetBackend registers the active backend. Tests may inject a mock; passing nil
// clears the backend.
func SetBackend(b Backend) {
	mu.Lock()
	defer mu.Unlock()
	backend = b
}

func getBackend() (Backend, error) {
	mu.RLock()
	defer mu.RUnlock()
	if backend == nil {
		return nil, errors.New("metric backend not registered")
	}
	return backend, nil
}

// RegisterTagKey registers a tag key that can be used on metrics.
func RegisterTagKey(key string) error {
	b, err := getBackend()
	if err != nil {
		return err
	}
	return b.RegisterTagKey(key)
}

// Metric is a registered metric handle.
//
// mu guards handle and registerErr so a Record racing an Unregister cannot
// observe a torn handle or record into an already-released metric. Record takes
// the read lock around its whole backend call (concurrent records proceed in
// parallel); Unregister takes the write lock, so the handle is set to -1 only
// after the backend has released it and no Record is in flight.
type Metric struct {
	mu          sync.RWMutex
	tagKeys     []string
	handle      int64
	registerErr error
}

// Record reports a value with the given tag values. Tag values are aligned with
// the tag keys the metric was registered with; missing keys map to the empty
// string, keeping keys and values slices the same length.
func (m *Metric) Record(tagValues map[string]string, value float64) error {
	b, err := getBackend()
	if err != nil {
		return err
	}
	vals := make([]string, 0, len(m.tagKeys))
	for _, k := range m.tagKeys {
		vals = append(vals, tagValues[k])
	}

	m.mu.RLock()
	defer m.mu.RUnlock()
	if m.registerErr != nil {
		return m.registerErr
	}
	if m.handle < 0 {
		return errors.New("metric not registered")
	}
	return b.Record(m.handle, value, m.tagKeys, vals)
}

// Unregister releases the metric in the backend.
func (m *Metric) Unregister() error {
	b, err := getBackend()
	if err != nil {
		return err
	}

	m.mu.Lock()
	defer m.mu.Unlock()
	// A failed registration is reported as-is, before the handle check: the
	// backend's failure sentinel is not part of the public contract (a backend
	// may return any non-nil error with any handle), so keying the error on the
	// handle value would hide the real cause behind a generic message.
	if m.registerErr != nil {
		return m.registerErr
	}
	if m.handle < 0 {
		return errors.New("metric not registered")
	}
	if err := b.Unregister(m.handle); err != nil {
		return err
	}
	m.handle = -1
	return nil
}

// Count is a monotonically increasing counter metric. Values reported via
// Record map to C++ stats::Count, which aggregates with the opencensus Count()
// aggregation (each Record call counts as one observation; the value is not
// summed).
type Count struct{ Metric }

// NewCount registers a count metric.
func NewCount(name, desc, unit string, tagKeys []string) *Count {
	handle, err := withBackend(func(b Backend) (int64, error) {
		return b.RegisterCount(name, desc, unit, tagKeys)
	})
	return &Count{Metric{tagKeys: tagKeys, handle: handle, registerErr: err}}
}

// Gauge is a value that can be set to a current reading. Values reported via
// Record overwrite the previous reading, mapping to C++ stats::Gauge (opencensus
// LastValue() aggregation).
type Gauge struct{ Metric }

// NewGauge registers a gauge metric.
func NewGauge(name, desc, unit string, tagKeys []string) *Gauge {
	handle, err := withBackend(func(b Backend) (int64, error) {
		return b.RegisterGauge(name, desc, unit, tagKeys)
	})
	return &Gauge{Metric{tagKeys: tagKeys, handle: handle, registerErr: err}}
}

// Sum is a metric whose value is the sum of all recorded values. Values
// reported via Record accumulate, mapping to C++ stats::Sum (opencensus Sum()
// aggregation).
type Sum struct{ Metric }

// NewSum registers a sum metric.
func NewSum(name, desc, unit string, tagKeys []string) *Sum {
	handle, err := withBackend(func(b Backend) (int64, error) {
		return b.RegisterSum(name, desc, unit, tagKeys)
	})
	return &Sum{Metric{tagKeys: tagKeys, handle: handle, registerErr: err}}
}

// Histogram is a metric whose values are bucketed into boundaries. Values
// reported via Record are observed as samples, mapping to C++ stats::Histogram
// (opencensus Distribution() aggregation).
type Histogram struct{ Metric }

// NewHistogram registers a histogram metric with the given boundaries.
func NewHistogram(name, desc, unit string, boundaries []float64, tagKeys []string) *Histogram {
	handle, err := withBackend(func(b Backend) (int64, error) {
		return b.RegisterHistogram(name, desc, unit, boundaries, tagKeys)
	})
	return &Histogram{Metric{tagKeys: tagKeys, handle: handle, registerErr: err}}
}

func withBackend(fn func(Backend) (int64, error)) (int64, error) {
	b, err := getBackend()
	if err != nil {
		return -1, err
	}
	return fn(b)
}
