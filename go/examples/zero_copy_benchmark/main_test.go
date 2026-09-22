// Copyright 2025 The Ray Authors.
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

package main

import (
	"bytes"
	"testing"
	"time"
)

func TestSizeFromStr(t *testing.T) {
	cases := []struct {
		in   string
		want int
		err  bool
	}{
		{"4MB", 4 * 1024 * 1024, false},
		{"64MB", 64 * 1024 * 1024, false},
		{"1KB", 1024, false},
		{"100B", 100, false},
		{"4096", 4096, false},
		{" 8MB ", 8 * 1024 * 1024, false},
		{"abc", 0, true},
		{"", 0, true},
	}
	for _, tc := range cases {
		got, err := sizeFromStr(tc.in)
		if tc.err {
			if err == nil {
				t.Errorf("sizeFromStr(%q) expected error, got nil", tc.in)
			}
			continue
		}
		if err != nil {
			t.Errorf("sizeFromStr(%q) unexpected error: %v", tc.in, err)
			continue
		}
		if got != tc.want {
			t.Errorf("sizeFromStr(%q) = %d, want %d", tc.in, got, tc.want)
		}
	}
}

func TestHumanSize(t *testing.T) {
	cases := []struct {
		in   int
		want string
	}{
		{4 * 1024 * 1024, "4MB"},
		{64 * 1024 * 1024, "64MB"},
		{1024, "1KB"},
		{123, "123B"},
	}
	for _, tc := range cases {
		if got := humanSize(tc.in); got != tc.want {
			t.Errorf("humanSize(%d) = %q, want %q", tc.in, got, tc.want)
		}
	}
}

func TestParseSizes(t *testing.T) {
	got, err := parseSizes("4MB,64MB,1KB")
	if err != nil {
		t.Fatalf("parseSizes unexpected error: %v", err)
	}
	want := []int{4 * 1024 * 1024, 64 * 1024 * 1024, 1024}
	if len(got) != len(want) {
		t.Fatalf("parseSizes len = %d, want %d", len(got), len(want))
	}
	for i := range want {
		if got[i] != want[i] {
			t.Errorf("parseSizes[%d] = %d, want %d", i, got[i], want[i])
		}
	}
	if _, err := parseSizes("4MB,abc"); err == nil {
		t.Error("parseSizes(\"4MB,abc\") expected error, got nil")
	}
}

func TestGenAndVerifyPayload(t *testing.T) {
	for _, size := range []int{0, 1024, 4 * 1024 * 1024} {
		p := genPayload(size)
		if len(p) != size {
			t.Fatalf("genPayload(%d) len = %d", size, len(p))
		}
		if err := verifyPayload(p, p); err != nil {
			t.Errorf("verifyPayload(same) failed for size %d: %v", size, err)
		}
		// Mutated copy must fail verification.
		if size > 0 {
			bad := make([]byte, size)
			copy(bad, p)
			bad[len(bad)-1] ^= 0xff
			if err := verifyPayload(bad, p); err == nil {
				t.Errorf("verifyPayload(mutated) should fail for size %d", size)
			}
		}
		// Length mismatch must fail.
		if size > 0 {
			if err := verifyPayload(p[:size-1], p); err == nil {
				t.Errorf("verifyPayload(short) should fail for size %d", size)
			}
		}
	}
}

func TestPercentile(t *testing.T) {
	// Single sample must not panic and must return that sample.
	single := []time.Duration{5 * time.Millisecond}
	if got := percentile(single, 0.95); got != 5*time.Millisecond {
		t.Errorf("percentile(single) = %v, want 5ms", got)
	}
	// Ascending samples: p0.5 should land near the middle.
	asc := make([]time.Duration, 20)
	for i := range asc {
		asc[i] = time.Duration(i+1) * time.Millisecond
	}
	if got := percentile(asc, 1.0); got != 20*time.Millisecond {
		t.Errorf("percentile(asc, 1.0) = %v, want 20ms", got)
	}
	if got := percentile(asc, 0.5); got < 9*time.Millisecond || got > 12*time.Millisecond {
		t.Errorf("percentile(asc, 0.5) = %v, out of expected [9,12]ms range", got)
	}
}

func TestComputeStats(t *testing.T) {
	lats := []time.Duration{
		10 * time.Millisecond,
		20 * time.Millisecond,
		30 * time.Millisecond,
		40 * time.Millisecond,
	}
	s := computeStats("local", 4*1024*1024, lats)
	if s.Mode != "local" {
		t.Errorf("Mode = %q, want local", s.Mode)
	}
	if s.Iterations != 4 {
		t.Errorf("Iterations = %d, want 4", s.Iterations)
	}
	if s.Mean != 25*time.Millisecond {
		t.Errorf("Mean = %v, want 25ms", s.Mean)
	}
	if s.Median != 30*time.Millisecond {
		t.Errorf("Median = %v, want 30ms", s.Median)
	}
	if s.ThroughputMB <= 0 {
		t.Errorf("ThroughputMB = %f, want > 0", s.ThroughputMB)
	}
}

func TestVerifyPayloadUsesDeterministicFill(t *testing.T) {
	// genPayload must be deterministic: two calls with same size give equal data.
	a := genPayload(4096)
	b := genPayload(4096)
	if !bytes.Equal(a, b) {
		t.Error("genPayload not deterministic")
	}
}
