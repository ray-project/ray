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

package logmonitor

import (
	"context"
	"testing"

	"github.com/ray-project/ray/go/pkg/gcs"
)

func TestRuntimePublisherBuildsPythonCompatiblePayload(t *testing.T) {
	batch := LogBatch{
		IP:        "127.0.0.1",
		PID:       "1234",
		JobID:     "01000000",
		IsErr:     true,
		Lines:     []string{"line-1", "line-2"},
		ActorName: "ActorA",
		TaskName:  "TaskA",
	}

	payload := buildPublishPayload(batch)
	if payload.PID != "1234" {
		t.Fatalf("pid = %#v, want %#v", payload.PID, "1234")
	}
}

type fakePublishLogsClient struct {
	payloads []gcs.LogBatchPayload
	err      error
}

func (f *fakePublishLogsClient) PublishLogBatch(_ context.Context, payload gcs.LogBatchPayload) error {
	f.payloads = append(f.payloads, payload)
	return f.err
}

func TestRuntimePublisherReturnsClientError(t *testing.T) {
	client := &fakePublishLogsClient{err: assertErr{}}
	publisher := NewRuntimePublisher(context.Background(), client)
	err := publisher.Publish(LogBatch{IP: "127.0.0.1", PID: "1", Lines: []string{"x"}})
	if err == nil {
		t.Fatal("expected publisher error")
	}
}

type assertErr struct{}

func (assertErr) Error() string { return "publish failed" }
