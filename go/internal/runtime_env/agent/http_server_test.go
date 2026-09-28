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
	"bytes"
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	runtime_env_agent_pb "github.com/ray-project/ray/go/proto"
	"google.golang.org/protobuf/proto"
)

// testMethodNotAllowed asserts that the handler returns 405 for disallowed HTTP methods.
func testMethodNotAllowed(t *testing.T, handlerFunc func(http.ResponseWriter, *http.Request), endpoint string) {
	t.Helper()
	req := httptest.NewRequest(http.MethodGet, endpoint, nil)
	w := httptest.NewRecorder()

	handlerFunc(w, req)

	if w.Code != http.StatusMethodNotAllowed {
		t.Errorf("endpoint %s: expected status %d, got %d", endpoint, http.StatusMethodNotAllowed, w.Code)
	}
}

// testInvalidProtobuf asserts that the handler returns the correct status code
// for an invalid protobuf request body.
func testInvalidProtobuf(t *testing.T, handlerFunc func(http.ResponseWriter, *http.Request), endpoint string) {
	t.Helper()
	body := []byte("invalid protobuf data")
	req := httptest.NewRequest(http.MethodPost, endpoint, bytes.NewReader(body))
	w := httptest.NewRecorder()

	handlerFunc(w, req)

	// Invalid protobuf data maps to 500 Internal Server Error because a failed
	// unmarshal is treated as a server error.
	if w.Code != http.StatusInternalServerError {
		t.Errorf("endpoint %s: expected status %d, got %d", endpoint, http.StatusInternalServerError, w.Code)
	}
}

// TestGetOrCreateRuntimeEnvHandler_InvalidProtobuf checks the status code for
// an invalid protobuf request body.
func TestGetOrCreateRuntimeEnvHandler_InvalidProtobuf(t *testing.T) {
	mockService := &MockRuntimeEnvAgentService{}
	server := NewHTTPServer(":0", mockService)
	testInvalidProtobuf(t, server.getOrCreateRuntimeEnvHandler, "/get_or_create_runtime_env")
}

func TestGetOrCreateRuntimeEnvHandler_MethodNotAllowed(t *testing.T) {
	mockService := &MockRuntimeEnvAgentService{}
	server := NewHTTPServer(":0", mockService)
	testMethodNotAllowed(t, server.getOrCreateRuntimeEnvHandler, "/get_or_create_runtime_env")
}

func TestDeleteRuntimeEnvIfPossibleHandler_MethodNotAllowed(t *testing.T) {
	mockService := &MockRuntimeEnvAgentService{}
	server := NewHTTPServer(":0", mockService)
	testMethodNotAllowed(t, server.deleteRuntimeEnvIfPossibleHandler, "/delete_runtime_env_if_possible")
}

func TestGetRuntimeEnvsInfoHandler_MethodNotAllowed(t *testing.T) {
	mockService := &MockRuntimeEnvAgentService{}
	server := NewHTTPServer(":0", mockService)
	testMethodNotAllowed(t, server.getRuntimeEnvsInfoHandler, "/get_runtime_envs_info")
}

// Mock service for testing
type MockRuntimeEnvAgentService struct{}

func (m *MockRuntimeEnvAgentService) GetOrCreateRuntimeEnv(ctx context.Context, request *runtime_env_agent_pb.GetOrCreateRuntimeEnvRequest) (*runtime_env_agent_pb.GetOrCreateRuntimeEnvReply, error) {
	return &runtime_env_agent_pb.GetOrCreateRuntimeEnvReply{
		Status:                      runtime_env_agent_pb.AgentRpcStatus_AGENT_RPC_STATUS_OK,
		SerializedRuntimeEnvContext: "{}",
	}, nil
}

func (m *MockRuntimeEnvAgentService) DeleteRuntimeEnvIfPossible(ctx context.Context, request *runtime_env_agent_pb.DeleteRuntimeEnvIfPossibleRequest) (*runtime_env_agent_pb.DeleteRuntimeEnvIfPossibleReply, error) {
	return &runtime_env_agent_pb.DeleteRuntimeEnvIfPossibleReply{
		Status: runtime_env_agent_pb.AgentRpcStatus_AGENT_RPC_STATUS_OK,
	}, nil
}

func (m *MockRuntimeEnvAgentService) GetRuntimeEnvsInfo(ctx context.Context, request *runtime_env_agent_pb.GetRuntimeEnvsInfoRequest) (*runtime_env_agent_pb.GetRuntimeEnvsInfoReply, error) {
	return &runtime_env_agent_pb.GetRuntimeEnvsInfoReply{
		Total: 0,
	}, nil
}

func TestGetOrCreateRuntimeEnvHandler_Integration(t *testing.T) {
	// Create the mock service.
	mockService := &MockRuntimeEnvAgentService{}

	server := NewHTTPServer(":0", mockService)

	req := &runtime_env_agent_pb.GetOrCreateRuntimeEnvRequest{
		JobId:                []byte("test_job_id"),
		SerializedRuntimeEnv: "{}",
		RuntimeEnvConfig:     &runtime_env_agent_pb.RuntimeEnvConfig{},
		SourceProcess:        "test",
	}
	body, err := proto.Marshal(req)
	if err != nil {
		t.Fatalf("failed to marshal request: %v", err)
	}

	httpReq := httptest.NewRequest(http.MethodPost, "/get_or_create_runtime_env", bytes.NewReader(body))
	w := httptest.NewRecorder()

	server.getOrCreateRuntimeEnvHandler(w, httpReq)

	if w.Code != http.StatusOK {
		t.Errorf("expected status 200, got %d: %s", w.Code, w.Body.String())
	}

	// Parse the response.
	var reply runtime_env_agent_pb.GetOrCreateRuntimeEnvReply
	if err := proto.Unmarshal(w.Body.Bytes(), &reply); err != nil {
		t.Fatalf("failed to unmarshal reply: %v", err)
	}

	if reply.Status != runtime_env_agent_pb.AgentRpcStatus_AGENT_RPC_STATUS_OK {
		t.Errorf("expected OK status, got: %v", reply.Status)
	}
}

func TestDeleteRuntimeEnvIfPossibleHandler_Integration(t *testing.T) {
	// Create the mock service.
	mockService := &MockRuntimeEnvAgentService{}

	server := NewHTTPServer(":0", mockService)

	req := &runtime_env_agent_pb.DeleteRuntimeEnvIfPossibleRequest{
		SerializedRuntimeEnv: "{}",
		SourceProcess:        "test",
	}
	body, err := proto.Marshal(req)
	if err != nil {
		t.Fatalf("failed to marshal request: %v", err)
	}

	httpReq := httptest.NewRequest(http.MethodPost, "/delete_runtime_env_if_possible", bytes.NewReader(body))
	w := httptest.NewRecorder()

	server.deleteRuntimeEnvIfPossibleHandler(w, httpReq)

	if w.Code != http.StatusOK {
		t.Errorf("expected status 200, got %d: %s", w.Code, w.Body.String())
	}

	// Parse the response.
	var reply runtime_env_agent_pb.DeleteRuntimeEnvIfPossibleReply
	if err := proto.Unmarshal(w.Body.Bytes(), &reply); err != nil {
		t.Fatalf("failed to unmarshal reply: %v", err)
	}

	if reply.Status != runtime_env_agent_pb.AgentRpcStatus_AGENT_RPC_STATUS_OK {
		t.Errorf("expected OK status, got: %v", reply.Status)
	}
}

func TestGetRuntimeEnvsInfoHandler_Integration(t *testing.T) {
	// Create the mock service.
	mockService := &MockRuntimeEnvAgentService{}

	server := NewHTTPServer(":0", mockService)

	req := &runtime_env_agent_pb.GetRuntimeEnvsInfoRequest{
		Limit: func() *int64 { v := int64(10); return &v }(),
	}
	body, err := proto.Marshal(req)
	if err != nil {
		t.Fatalf("failed to marshal request: %v", err)
	}

	httpReq := httptest.NewRequest(http.MethodPost, "/get_runtime_envs_info", bytes.NewReader(body))
	w := httptest.NewRecorder()

	server.getRuntimeEnvsInfoHandler(w, httpReq)

	if w.Code != http.StatusOK {
		t.Errorf("expected status 200, got %d: %s", w.Code, w.Body.String())
	}

	// Parse the response.
	var reply runtime_env_agent_pb.GetRuntimeEnvsInfoReply
	if err := proto.Unmarshal(w.Body.Bytes(), &reply); err != nil {
		t.Fatalf("failed to unmarshal reply: %v", err)
	}
}

func TestGetRuntimeEnvsInfoHandler_InvalidProtobuf(t *testing.T) {
	mockService := &MockRuntimeEnvAgentService{}
	server := NewHTTPServer(":0", mockService)
	testInvalidProtobuf(t, server.getRuntimeEnvsInfoHandler, "/get_runtime_envs_info")
}

func TestDeleteRuntimeEnvIfPossibleHandler_InvalidProtobuf(t *testing.T) {
	mockService := &MockRuntimeEnvAgentService{}
	server := NewHTTPServer(":0", mockService)
	testInvalidProtobuf(t, server.deleteRuntimeEnvIfPossibleHandler, "/delete_runtime_env_if_possible")
}

func TestHTTPServer_Start(t *testing.T) {
	mockService := &MockRuntimeEnvAgentService{}
	ctx, cancel := context.WithCancel(context.Background())
	server := NewHTTPServer(":0", mockService)

	// Start the server.
	go func() {
		_ = server.Start(ctx)
	}()

	// Cancel immediately to trigger shutdown.
	cancel()
}
