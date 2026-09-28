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
	"fmt"
	"io"
	"net"
	"net/http"

	"google.golang.org/protobuf/proto"

	runtime_env_agent_pb "github.com/ray-project/ray/go/proto"
)

// HTTPServer wraps the agent's HTTP server.
type HTTPServer struct {
	server  *http.Server
	service RuntimeEnvAgentServiceInterface
	// OnBound, when set, is invoked with the actually bound port right
	// after the listener is created. The raylet discovers this agent by
	// waiting for the port to be persisted to the session directory
	// (WaitForPersistedPort), so a port of 0 (OS-assigned) must be
	// reported back through this callback.
	OnBound func(port int)
}

// RuntimeEnvAgentServiceInterface is the service interface exposed by the agent.
type RuntimeEnvAgentServiceInterface interface {
	GetOrCreateRuntimeEnv(ctx context.Context, request *runtime_env_agent_pb.GetOrCreateRuntimeEnvRequest) (*runtime_env_agent_pb.GetOrCreateRuntimeEnvReply, error)
	DeleteRuntimeEnvIfPossible(ctx context.Context, request *runtime_env_agent_pb.DeleteRuntimeEnvIfPossibleRequest) (*runtime_env_agent_pb.DeleteRuntimeEnvIfPossibleReply, error)
	GetRuntimeEnvsInfo(ctx context.Context, request *runtime_env_agent_pb.GetRuntimeEnvsInfoRequest) (*runtime_env_agent_pb.GetRuntimeEnvsInfoReply, error)
}

// handleProtobufRequest is the shared binary-protobuf request handler.
// handlerFunc receives the raw request bytes and the request context, and
// returns the response bytes and an error.
func handleProtobufRequest(w http.ResponseWriter, r *http.Request, handlerFunc func([]byte, context.Context) ([]byte, error)) {
	// Ensure the request body is closed on every exit path.
	defer r.Body.Close()

	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	const maxRequestBodySize = 10 << 20 // 10MB

	// Cap the request body size with MaxBytesReader; oversized bodies error out.
	r.Body = http.MaxBytesReader(w, r.Body, maxRequestBodySize)

	body, err := io.ReadAll(r.Body)
	if err != nil {
		// Distinguish the over-size-limit error.
		if maxBytesErr, ok := err.(*http.MaxBytesError); ok {
			http.Error(w, "Request body too large: "+maxBytesErr.Error(), http.StatusRequestEntityTooLarge)
			return
		}
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	// Use the request context so the handler can observe client cancellation.
	responseBytes, err := handlerFunc(body, r.Context())
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/octet-stream")
	w.Write(responseBytes)
}

// createProtobufHandler builds a generic protobuf handler.
// T is the request type, U the response type.
func createProtobufHandler[T proto.Message, U proto.Message](
	req T,
	serviceCall func(context.Context, T) (U, error),
) func([]byte, context.Context) ([]byte, error) {
	return func(body []byte, ctx context.Context) ([]byte, error) {
		// Create a fresh request instance.
		reqCopy := proto.Clone(req).(T)
		if err := proto.Unmarshal(body, reqCopy); err != nil {
			return nil, fmt.Errorf("failed to unmarshal request: %w", err)
		}

		resp, err := serviceCall(ctx, reqCopy)
		if err != nil {
			return nil, err
		}

		respBytes, err := proto.Marshal(resp)
		if err != nil {
			return nil, fmt.Errorf("failed to marshal response: %w", err)
		}
		return respBytes, nil
	}
}

// NewHTTPServer creates a new HTTP server.
func NewHTTPServer(addr string, service RuntimeEnvAgentServiceInterface) *HTTPServer {
	mux := http.NewServeMux()
	s := &HTTPServer{
		service: service,
	}
	mux.HandleFunc("/get_or_create_runtime_env", s.getOrCreateRuntimeEnvHandler)
	mux.HandleFunc("/delete_runtime_env_if_possible", s.deleteRuntimeEnvIfPossibleHandler)
	mux.HandleFunc("/get_runtime_envs_info", s.getRuntimeEnvsInfoHandler)

	s.server = &http.Server{
		Addr:    addr,
		Handler: mux,
	}
	return s
}

// Start runs the HTTP server.
func (s *HTTPServer) Start(ctx context.Context) error {
	// Bind explicitly so the actually bound port is known: with
	// --runtime-env-agent-port=0 the OS assigns one, and the raylet can only
	// discover this agent after the port is persisted (see OnBound).
	listener, err := net.Listen("tcp", s.server.Addr)
	if err != nil {
		return fmt.Errorf("failed to bind %s: %w", s.server.Addr, err)
	}
	if s.OnBound != nil {
		s.OnBound(listener.Addr().(*net.TCPAddr).Port)
	}

	// Serve in a goroutine.
	errChan := make(chan error, 1)
	go func() {
		if err := s.server.Serve(listener); err != nil && err != http.ErrServerClosed {
			errChan <- err
		}
		close(errChan)
	}()

	// Wait for context cancellation or a server error.
	select {
	case <-ctx.Done():
		return s.server.Shutdown(context.Background())
	case err := <-errChan:
		return err
	}
}

// getOrCreateRuntimeEnvHandler serves get-or-create runtime env requests.
func (s *HTTPServer) getOrCreateRuntimeEnvHandler(w http.ResponseWriter, r *http.Request) {
	handler := createProtobufHandler(
		&runtime_env_agent_pb.GetOrCreateRuntimeEnvRequest{},
		s.service.GetOrCreateRuntimeEnv,
	)
	handleProtobufRequest(w, r, handler)
}

// deleteRuntimeEnvIfPossibleHandler serves delete-runtime-env requests.
func (s *HTTPServer) deleteRuntimeEnvIfPossibleHandler(w http.ResponseWriter, r *http.Request) {
	handler := createProtobufHandler(
		&runtime_env_agent_pb.DeleteRuntimeEnvIfPossibleRequest{},
		s.service.DeleteRuntimeEnvIfPossible,
	)
	handleProtobufRequest(w, r, handler)
}

// getRuntimeEnvsInfoHandler serves get-runtime-envs-info requests.
func (s *HTTPServer) getRuntimeEnvsInfoHandler(w http.ResponseWriter, r *http.Request) {
	handler := createProtobufHandler(
		&runtime_env_agent_pb.GetRuntimeEnvsInfoRequest{},
		s.service.GetRuntimeEnvsInfo,
	)
	handleProtobufRequest(w, r, handler)
}
