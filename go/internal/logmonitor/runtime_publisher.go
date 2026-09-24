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

	"github.com/ray-project/ray/go/pkg/gcs"
)

type publishLogsClient interface {
	PublishLogBatch(ctx context.Context, payload gcs.LogBatchPayload) error
}

type runtimePublisher struct {
	ctx    context.Context
	client publishLogsClient
}

func NewRuntimePublisher(ctx context.Context, client publishLogsClient) Publisher {
	if ctx == nil {
		ctx = context.Background()
	}
	return &runtimePublisher{ctx: ctx, client: client}
}

func (p *runtimePublisher) Publish(batch LogBatch) error {
	return p.client.PublishLogBatch(p.ctx, buildPublishPayload(batch))
}

func buildPublishPayload(batch LogBatch) gcs.LogBatchPayload {
	return gcs.LogBatchPayload{
		IP:        batch.IP,
		PID:       batch.PID,
		JobID:     batch.JobID,
		IsError:   batch.IsErr,
		Lines:     append([]string(nil), batch.Lines...),
		ActorName: batch.ActorName,
		TaskName:  batch.TaskName,
	}
}
