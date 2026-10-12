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

package metric

import (
	"errors"
	"sync"
	"testing"
)

type mockBackend struct {
	mu          sync.Mutex
	registered  []string
	regTypes    []string
	recorded    int
	unregisters int
	tagKeyCalls int
	lastTagKey  string
	lastHandle  int64
	lastValue   float64
	lastKeys    []string
	lastVals    []string
	registerErr error
}

func (m *mockBackend) RegisterTagKey(key string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.tagKeyCalls++
	m.lastTagKey = key
	return nil
}

func (m *mockBackend) RegisterCount(name, desc, unit string, tagKeys []string) (int64, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.registerErr != nil {
		return -1, m.registerErr
	}
	m.registered = append(m.registered, name)
	m.regTypes = append(m.regTypes, "count")
	return 1, nil
}

func (m *mockBackend) RegisterGauge(name, desc, unit string, tagKeys []string) (int64, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.registerErr != nil {
		return -1, m.registerErr
	}
	m.registered = append(m.registered, name)
	m.regTypes = append(m.regTypes, "gauge")
	return 2, nil
}

func (m *mockBackend) RegisterSum(name, desc, unit string, tagKeys []string) (int64, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.registerErr != nil {
		return -1, m.registerErr
	}
	m.registered = append(m.registered, name)
	m.regTypes = append(m.regTypes, "sum")
	return 3, nil
}

func (m *mockBackend) RegisterHistogram(name, desc, unit string, boundaries []float64, tagKeys []string) (int64, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.registerErr != nil {
		return -1, m.registerErr
	}
	m.registered = append(m.registered, name)
	m.regTypes = append(m.regTypes, "histogram")
	return 4, nil
}

func (m *mockBackend) Record(handle int64, value float64, tagKeys, tagValues []string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.recorded++
	m.lastHandle = handle
	m.lastValue = value
	m.lastKeys = append([]string(nil), tagKeys...)
	m.lastVals = append([]string(nil), tagValues...)
	return nil
}

func (m *mockBackend) Unregister(handle int64) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.unregisters++
	return nil
}

func TestCountRecordUnregister(t *testing.T) {
	mb := &mockBackend{}
	SetBackend(mb)
	defer SetBackend(nil)

	c := NewCount("my_count", "a counter", "count", []string{"k1"})
	if err := c.Record(map[string]string{"k1": "v1"}, 1.0); err != nil {
		t.Fatalf("Record error: %v", err)
	}
	if err := c.Unregister(); err != nil {
		t.Fatalf("Unregister error: %v", err)
	}
	mb.mu.Lock()
	defer mb.mu.Unlock()
	if len(mb.registered) != 1 || mb.registered[0] != "my_count" {
		t.Fatalf("unexpected registered: %v", mb.registered)
	}
	if mb.recorded != 1 || mb.unregisters != 1 {
		t.Fatalf("recorded=%d unregisters=%d", mb.recorded, mb.unregisters)
	}
	if mb.lastHandle != 1 || mb.lastValue != 1.0 {
		t.Fatalf("lastHandle=%d lastValue=%v", mb.lastHandle, mb.lastValue)
	}
	if len(mb.lastKeys) != 1 || mb.lastKeys[0] != "k1" {
		t.Fatalf("lastKeys=%v", mb.lastKeys)
	}
	if len(mb.lastVals) != 1 || mb.lastVals[0] != "v1" {
		t.Fatalf("lastVals=%v", mb.lastVals)
	}
}

func TestRecordTagAlignment(t *testing.T) {
	mb := &mockBackend{}
	SetBackend(mb)
	defer SetBackend(nil)

	c := NewCount("align_count", "a counter", "count", []string{"k1", "k2"})
	if err := c.Record(map[string]string{"k1": "v1"}, 5.0); err != nil {
		t.Fatalf("Record error: %v", err)
	}
	mb.mu.Lock()
	defer mb.mu.Unlock()
	wantKeys := []string{"k1", "k2"}
	wantVals := []string{"v1", ""}
	if len(mb.lastKeys) != len(wantKeys) || len(mb.lastVals) != len(wantVals) {
		t.Fatalf("length mismatch keys=%v vals=%v", mb.lastKeys, mb.lastVals)
	}
	if len(mb.lastKeys) != len(mb.lastVals) {
		t.Fatalf("keys and vals not equal length: %v vs %v", mb.lastKeys, mb.lastVals)
	}
	for i := range wantKeys {
		if mb.lastKeys[i] != wantKeys[i] {
			t.Fatalf("keys[%d]=%q want %q", i, mb.lastKeys[i], wantKeys[i])
		}
	}
	for i := range wantVals {
		if mb.lastVals[i] != wantVals[i] {
			t.Fatalf("vals[%d]=%q want %q", i, mb.lastVals[i], wantVals[i])
		}
	}
}

func TestRecordWithoutBackend(t *testing.T) {
	SetBackend(nil)
	defer SetBackend(nil)

	c := NewCount("no_backend_count", "a counter", "count", []string{"k1"})
	if err := c.Record(map[string]string{"k1": "v1"}, 1.0); err == nil {
		t.Fatalf("expected error recording without backend, got nil")
	}
}

func TestConstructWithoutBackendThenRecord(t *testing.T) {
	SetBackend(nil)
	defer SetBackend(nil)

	c := NewCount("late_backend_count", "a counter", "count", []string{"k1"})

	mb := &mockBackend{}
	SetBackend(mb)
	if err := c.Record(map[string]string{"k1": "v1"}, 1.0); err == nil {
		t.Fatalf("expected 'metric not registered' error for handle=-1, got nil")
	}
	if err := c.Unregister(); err == nil {
		t.Fatalf("expected 'metric not registered' error for handle=-1, got nil")
	}
	mb.mu.Lock()
	defer mb.mu.Unlock()
	if mb.recorded != 0 {
		t.Fatalf("backend should not receive Record for handle=-1, recorded=%d", mb.recorded)
	}
	if mb.unregisters != 0 {
		t.Fatalf("backend should not receive Unregister for handle=-1, unregisters=%d", mb.unregisters)
	}
}

func TestRegisterErrorPropagatedToRecordAndUnregister(t *testing.T) {
	registerErr := errors.New("metric backend not registered")
	mb := &mockBackend{registerErr: registerErr}
	SetBackend(mb)
	defer SetBackend(nil)

	for _, c := range []*Metric{
		&NewCount("err_count", "a counter", "count", nil).Metric,
		&NewGauge("err_gauge", "a gauge", "g", nil).Metric,
		&NewSum("err_sum", "a sum", "s", nil).Metric,
		&NewHistogram("err_hist", "a histogram", "h", []float64{1}, nil).Metric,
	} {
		if err := c.Record(map[string]string{}, 1.0); err != registerErr {
			t.Fatalf("Record error = %v, want %v", err, registerErr)
		}
		if err := c.Unregister(); err != registerErr {
			t.Fatalf("Unregister error = %v, want %v", err, registerErr)
		}
	}
	mb.mu.Lock()
	defer mb.mu.Unlock()
	if len(mb.registered) != 0 {
		t.Fatalf("failed registrations should not be recorded, got %v", mb.registered)
	}
	if mb.recorded != 0 || mb.unregisters != 0 {
		t.Fatalf("backend must not see Record/Unregister for failed registrations, recorded=%d unregisters=%d", mb.recorded, mb.unregisters)
	}
}

func TestRegisterTagKey(t *testing.T) {
	mb := &mockBackend{}
	SetBackend(mb)
	defer SetBackend(nil)

	if err := RegisterTagKey("k1"); err != nil {
		t.Fatalf("RegisterTagKey error: %v", err)
	}
	mb.mu.Lock()
	defer mb.mu.Unlock()
	if mb.tagKeyCalls != 1 || mb.lastTagKey != "k1" {
		t.Fatalf("tagKeyCalls=%d lastTagKey=%q", mb.tagKeyCalls, mb.lastTagKey)
	}
}

func TestRegisterMetricTypes(t *testing.T) {
	mb := &mockBackend{}
	SetBackend(mb)
	defer SetBackend(nil)

	NewGauge("my_gauge", "a gauge", "g", nil)
	NewSum("my_sum", "a sum", "s", nil)
	NewHistogram("my_hist", "a histogram", "h", []float64{1, 2, 3}, nil)

	mb.mu.Lock()
	defer mb.mu.Unlock()
	want := []string{"gauge", "sum", "histogram"}
	if len(mb.regTypes) != len(want) {
		t.Fatalf("regTypes=%v want %v", mb.regTypes, want)
	}
	for i := range want {
		if mb.regTypes[i] != want[i] {
			t.Fatalf("regTypes[%d]=%q want %q", i, mb.regTypes[i], want[i])
		}
	}
}

// zeroHandleErrBackend mimics the cgo backend's failure convention: it reports
// a registration failure as (0, err). The public package must surface that err
// regardless of the handle value -- keying the error on handle < 0 would let a
// 0 handle slip through as "registered" and hide the real cause behind a
// generic "unknown metric handle" from every later Record.
type zeroHandleErrBackend struct {
	mockBackend
	regErr error
}

func (z *zeroHandleErrBackend) RegisterCount(name, desc, unit string, tagKeys []string) (int64, error) {
	return 0, z.regErr
}

func TestRegistrationErrorSurfacesForNonNegativeHandle(t *testing.T) {
	sentinel := errors.New("invalid metric name")
	SetBackend(&zeroHandleErrBackend{regErr: sentinel})
	defer SetBackend(nil)

	c := NewCount("1-bad-name", "desc", "count", nil)
	if err := c.Record(map[string]string{}, 1); !errors.Is(err, sentinel) {
		t.Fatalf("Record on a failed registration should surface the register error, got %v", err)
	}
	if err := c.Unregister(); !errors.Is(err, sentinel) {
		t.Fatalf("Unregister on a failed registration should surface the register error, got %v", err)
	}
}

// TestMetricConcurrentRecordUnregister exercises the exported race the pre-lock
// implementation had: a concurrent Record and Unregister on the same Metric.
// Run with -race, it fails (read/write of handle) before the RWMutex guard.
func TestMetricConcurrentRecordUnregister(t *testing.T) {
	SetBackend(&mockBackend{})
	defer SetBackend(nil)

	c := NewCount("concurrent", "desc", "count", []string{"k"})
	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_ = c.Record(map[string]string{"k": "v"}, 1)
		}()
	}
	wg.Add(1)
	go func() {
		defer wg.Done()
		_ = c.Unregister()
	}()
	wg.Wait()
}
