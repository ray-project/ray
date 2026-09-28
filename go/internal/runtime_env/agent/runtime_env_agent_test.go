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

package agent

import (
	"context"
	"testing"

	"github.com/ray-project/ray/go/internal/runtime_env"
	"github.com/ray-project/ray/go/pkg/gcs/mockgcs"
	"github.com/ray-project/ray/go/proto"
)

func newTestService() *RuntimeEnvAgentService {
	config := &RuntimeEnvAgentConfig{
		GcsClient:     &mockgcs.Client{},
		Address:       "127.0.0.1",
		LoggingParams: &LoggingConfig{},
	}
	service, err := NewRuntimeEnvAgentService(config)
	if err != nil {
		panic(err)
	}
	return service
}

func TestGetOrCreateRuntimeEnv_EmptyEnv(t *testing.T) {
	ctx := context.Background()
	service := newTestService()

	request := &proto.GetOrCreateRuntimeEnvRequest{
		JobId:                []byte("test_job"),
		SerializedRuntimeEnv: "{}",
		RuntimeEnvConfig:     &proto.RuntimeEnvConfig{},
		SourceProcess:        "test",
	}
	reply, err := service.GetOrCreateRuntimeEnv(ctx, request)

	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if reply.Status != proto.AgentRpcStatus_AGENT_RPC_STATUS_OK {
		t.Errorf("expected OK status, got: %v, error message: %s", reply.Status, reply.ErrorMessage)
	}
}

func TestGetOrCreateRuntimeEnv_InvalidJSON(t *testing.T) {
	ctx := context.Background()
	service := newTestService()

	request := &proto.GetOrCreateRuntimeEnvRequest{
		JobId:                []byte("test_job"),
		SerializedRuntimeEnv: "invalid json",
		RuntimeEnvConfig:     &proto.RuntimeEnvConfig{},
		SourceProcess:        "test",
	}
	reply, err := service.GetOrCreateRuntimeEnv(ctx, request)

	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if reply.Status != proto.AgentRpcStatus_AGENT_RPC_STATUS_FAILED {
		t.Errorf("expected FAILED status for invalid JSON, got: %v", reply.Status)
	}
}

func TestDeleteRuntimeEnvIfPossible_NonExistent(t *testing.T) {
	ctx := context.Background()
	service := newTestService()

	request := &proto.DeleteRuntimeEnvIfPossibleRequest{
		SerializedRuntimeEnv: "{}",
		SourceProcess:        "test",
	}
	reply, err := service.DeleteRuntimeEnvIfPossible(ctx, request)

	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// A non-existent runtime env should yield a failure.
	if reply.Status != proto.AgentRpcStatus_AGENT_RPC_STATUS_FAILED {
		t.Errorf("expected FAILED status for non-existent env, got: %v", reply.Status)
	}
}

func TestGetRuntimeEnvsInfo_Empty(t *testing.T) {
	ctx := context.Background()
	service := newTestService()

	request := &proto.GetRuntimeEnvsInfoRequest{}
	reply, err := service.GetRuntimeEnvsInfo(ctx, request)

	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if reply.Total != 0 {
		t.Errorf("expected 0 total envs, got: %d", reply.Total)
	}
}

func TestGetOrCreateRuntimeEnv_CachedSuccess(t *testing.T) {
	ctx := context.Background()
	service := newTestService()

	request := &proto.GetOrCreateRuntimeEnvRequest{
		JobId:                []byte("test_job"),
		SerializedRuntimeEnv: "{}",
		RuntimeEnvConfig:     &proto.RuntimeEnvConfig{},
		SourceProcess:        "test",
	}

	// The first call creates the env.
	reply1, err := service.GetOrCreateRuntimeEnv(ctx, request)

	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if reply1.Status != proto.AgentRpcStatus_AGENT_RPC_STATUS_OK {
		t.Errorf("expected OK status, got: %v", reply1.Status)
	}

	// The second call is served from the cache.
	reply2, err := service.GetOrCreateRuntimeEnv(ctx, request)

	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if reply2.Status != proto.AgentRpcStatus_AGENT_RPC_STATUS_OK {
		t.Errorf("expected OK status, got: %v", reply2.Status)
	}
}

func TestDeleteRuntimeEnvIfPossible_Success(t *testing.T) {
	ctx := context.Background()
	service := newTestService()

	// Create an env first.
	createRequest := &proto.GetOrCreateRuntimeEnvRequest{
		JobId:                []byte("test_job"),
		SerializedRuntimeEnv: "{}",
		RuntimeEnvConfig:     &proto.RuntimeEnvConfig{},
		SourceProcess:        "test",
	}
	_, _ = service.GetOrCreateRuntimeEnv(ctx, createRequest)

	// Then delete it.
	deleteRequest := &proto.DeleteRuntimeEnvIfPossibleRequest{
		SerializedRuntimeEnv: "{}",
		SourceProcess:        "test",
	}
	reply, err := service.DeleteRuntimeEnvIfPossible(ctx, deleteRequest)

	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if reply.Status != proto.AgentRpcStatus_AGENT_RPC_STATUS_OK {
		t.Errorf("expected OK status, got: %v", reply.Status)
	}
}

func TestDeleteRuntimeEnvIfPossible_InvalidJSON(t *testing.T) {
	ctx := context.Background()
	service := newTestService()

	request := &proto.DeleteRuntimeEnvIfPossibleRequest{
		SerializedRuntimeEnv: "invalid json",
		SourceProcess:        "test",
	}
	reply, err := service.DeleteRuntimeEnvIfPossible(ctx, request)

	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if reply.Status != proto.AgentRpcStatus_AGENT_RPC_STATUS_FAILED {
		t.Errorf("expected FAILED status, got: %v", reply.Status)
	}
}

func TestGetRuntimeEnvsInfo_WithLimit(t *testing.T) {
	ctx := context.Background()
	service := newTestService()

	request := &proto.GetOrCreateRuntimeEnvRequest{
		JobId:                []byte("test_job"),
		SerializedRuntimeEnv: "{}",
		RuntimeEnvConfig:     &proto.RuntimeEnvConfig{},
		SourceProcess:        "test",
	}

	// Create a few envs.
	for i := 0; i < 5; i++ {
		_, _ = service.GetOrCreateRuntimeEnv(ctx, request)
	}

	// Query the env info.
	infoRequest := &proto.GetRuntimeEnvsInfoRequest{}
	reply, err := service.GetRuntimeEnvsInfo(ctx, infoRequest)

	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// All requests share the same serialized env, so only one exists.
	if reply.Total != 1 {
		t.Errorf("expected 1 total envs, got: %d", reply.Total)
	}

	if int64(len(reply.RuntimeEnvStates)) > 2 {
		t.Errorf("expected at most 2 states, got: %d", len(reply.RuntimeEnvStates))
	}
}

func TestGetRuntimeEnvsInfo_WithMultipleEnvs(t *testing.T) {
	ctx := context.Background()
	service := newTestService()

	envs := []string{
		`{"env_vars": {"VAR1": "val1"}}`,
		`{"env_vars": {"VAR2": "val2"}}`,
		`{"env_vars": {"VAR3": "val3"}}`,
	}

	for _, env := range envs {
		request := &proto.GetOrCreateRuntimeEnvRequest{
			JobId:                []byte("test_job"),
			SerializedRuntimeEnv: env,
			RuntimeEnvConfig:     &proto.RuntimeEnvConfig{},
			SourceProcess:        "test",
		}
		reply, err := service.GetOrCreateRuntimeEnv(ctx, request)
		if err != nil {
			t.Fatalf("GetOrCreateRuntimeEnv failed: %v", err)
		}
		if reply.Status != proto.AgentRpcStatus_AGENT_RPC_STATUS_OK {
			t.Fatalf("Expected OK status, got: %v, error: %s", reply.Status, reply.ErrorMessage)
		}
	}

	request := &proto.GetRuntimeEnvsInfoRequest{}
	reply, err := service.GetRuntimeEnvsInfo(ctx, request)

	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if reply.Total != 3 {
		t.Errorf("expected 3 total envs, got: %d", reply.Total)
	}
}

func TestURIsParser_EmptyPlugins(t *testing.T) {
	service := newTestService()

	// Build an empty runtime env.
	env := &runtime_env.RuntimeEnv{}

	uris := service.urisParser(env)

	if len(uris) != 0 {
		t.Errorf("expected 0 URIs, got: %d", len(uris))
	}
}

func TestUnusedURIsProcessor(t *testing.T) {
	service := newTestService()

	uris := []URIWithType{
		{URI: "test_uri", URIType: "test_type"},
	}

	// Must not panic.
	service.unusedURIsProcessor(uris)
}

func TestUnusedRuntimeEnvProcessor(t *testing.T) {
	service := newTestService()

	request := &proto.GetOrCreateRuntimeEnvRequest{
		JobId:                []byte("test_job"),
		SerializedRuntimeEnv: "{}",
		RuntimeEnvConfig:     &proto.RuntimeEnvConfig{},
		SourceProcess:        "test",
	}
	_, _ = service.GetOrCreateRuntimeEnv(context.Background(), request)

	// Delete the env.
	service.unusedRuntimeEnvProcessor("{}")

	// The env must be gone from the cache.
	if _, exists := service.envCache.Load("{}"); exists {
		t.Error("expected env to be deleted from cache")
	}
}
