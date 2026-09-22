// Copyright 2025 The Ray Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package gcs

import (
	"errors"
	"testing"
)

// mockClient is a minimal Client implementation used by the singleton tests.
type mockClient struct {
	Client
	address string
}

func (m *mockClient) Address() string { return m.address }

// mockGlobalStateAccessor is a minimal GlobalStateAccessor implementation used
// by the singleton tests.
type mockGlobalStateAccessor struct {
	GlobalStateAccessor
}

func TestGetClientBeforeSet(t *testing.T) {
	ClearClient()
	client, err := GetClient()
	if err == nil {
		t.Fatal("expected error when client not set")
	}
	if !errors.Is(err, ErrNotImplemented) {
		t.Errorf("expected ErrNotImplemented, got %v", err)
	}
	if client != nil {
		t.Errorf("expected nil client, got %v", client)
	}
}

func TestSetAndGetClient(t *testing.T) {
	ClearClient()
	mock := &mockClient{address: "localhost:6379"}
	SetClient(mock)

	client, err := GetClient()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if client == nil {
		t.Fatal("expected non-nil client")
	}
	if client.Address() != "localhost:6379" {
		t.Errorf("expected address localhost:6379, got %s", client.Address())
	}
}

func TestSetClientOnce(t *testing.T) {
	ClearClient()
	first := &mockClient{address: "first"}
	second := &mockClient{address: "second"}

	SetClient(first)
	SetClient(second)

	client, err := GetClient()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if client.Address() != "first" {
		t.Errorf("expected first client to win, got %s", client.Address())
	}
}

func TestClearClient(t *testing.T) {
	ClearClient()
	SetClient(&mockClient{address: "localhost"})

	if _, err := GetClient(); err != nil {
		t.Fatalf("expected client set, got error: %v", err)
	}

	ClearClient()
	if _, err := GetClient(); !errors.Is(err, ErrNotImplemented) {
		t.Errorf("expected ErrNotImplemented after ClearClient, got %v", err)
	}

	// The client can be set again after ClearClient.
	SetClient(&mockClient{address: "again"})
	client, err := GetClient()
	if err != nil {
		t.Fatalf("expected client set again, got error: %v", err)
	}
	if client.Address() != "again" {
		t.Errorf("expected address again, got %s", client.Address())
	}
}

func TestGetGlobalStateAccessorBeforeSet(t *testing.T) {
	stateAccessorInstance = nil
	accessor, err := GetGlobalStateAccessor()
	if err == nil {
		t.Fatal("expected error when accessor not set")
	}
	if !errors.Is(err, ErrNotImplemented) {
		t.Errorf("expected ErrNotImplemented, got %v", err)
	}
	if accessor != nil {
		t.Errorf("expected nil accessor, got %v", accessor)
	}
}

func TestSetAndGetGlobalStateAccessor(t *testing.T) {
	stateAccessorInstance = nil
	mock := &mockGlobalStateAccessor{}
	SetGlobalStateAccessor(mock)

	accessor, err := GetGlobalStateAccessor()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if accessor == nil {
		t.Fatal("expected non-nil accessor")
	}
}
