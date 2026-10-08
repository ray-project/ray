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

package subscribers

import (
	"errors"
	"testing"

	"github.com/ray-project/ray/go/internal/autoscaler"
	"github.com/ray-project/ray/go/proto"
	"github.com/stretchr/testify/assert"
)

// fakeCloudProvider is a fake cloud provider recording the call arguments.
type fakeCloudProvider struct {
	launchCalls    []launchCall
	terminateCalls []terminateCall
	launchErr      error
	terminateErr   error
}

type launchCall struct {
	shape     map[string]int
	requestId string
}

type terminateCall struct {
	ids       []string
	requestId string
}

func (f *fakeCloudProvider) GetNonTerminated() (map[string]autoscaler.CloudInstance, error) {
	return nil, nil
}

func (f *fakeCloudProvider) Launch(shape map[string]int, requestId string) error {
	f.launchCalls = append(f.launchCalls, launchCall{shape: shape, requestId: requestId})
	return f.launchErr
}

func (f *fakeCloudProvider) Terminate(ids []string, requestId string) error {
	f.terminateCalls = append(f.terminateCalls, terminateCall{ids: ids, requestId: requestId})
	return f.terminateErr
}

func (f *fakeCloudProvider) PollErrors() []error {
	return nil
}

// strPtr returns a string pointer (proto optional fields are *string).
func strPtr(s string) *string { return &s }

// newEvent builds an event for tests.
func newEvent(status proto.Instance_InstanceStatus, instanceType, launchRequestId, cloudInstanceId string) *proto.InstanceUpdateEvent {
	return &proto.InstanceUpdateEvent{
		InstanceId:        "i-" + instanceType,
		NewInstanceStatus: status,
		InstanceType:      strPtr(instanceType),
		LaunchRequestId:   strPtr(launchRequestId),
		CloudInstanceId:   strPtr(cloudInstanceId),
	}
}

// TestCloudInstanceUpdater_Launch groups and aggregates REQUESTED events by
// launch request id and launches.
func TestCloudInstanceUpdater_Launch(t *testing.T) {
	provider := &fakeCloudProvider{}
	updater := NewCloudInstanceUpdater(provider)

	updater.Notify([]*proto.InstanceUpdateEvent{
		newEvent(proto.Instance_REQUESTED, "type-a", "req-1", ""),
		newEvent(proto.Instance_REQUESTED, "type-a", "req-1", ""),
		newEvent(proto.Instance_REQUESTED, "type-b", "req-1", ""),
		newEvent(proto.Instance_REQUESTED, "type-a", "req-2", ""),
	})

	// Grouped by launch request id: req-1 has 2 type-a and 1 type-b; req-2 has
	// 1 type-a.
	assert.Len(t, provider.launchCalls, 2)
	callsByReq := make(map[string]launchCall)
	for _, call := range provider.launchCalls {
		callsByReq[call.requestId] = call
	}
	assert.Equal(t, map[string]int{"type-a": 2, "type-b": 1}, callsByReq["req-1"].shape)
	assert.Equal(t, map[string]int{"type-a": 1}, callsByReq["req-2"].shape)
	// No terminate calls.
	assert.Empty(t, provider.terminateCalls)
}

// TestCloudInstanceUpdater_LaunchWithoutRequestId events without a launch
// request id are skipped.
func TestCloudInstanceUpdater_LaunchWithoutRequestId(t *testing.T) {
	provider := &fakeCloudProvider{}
	updater := NewCloudInstanceUpdater(provider)

	updater.Notify([]*proto.InstanceUpdateEvent{
		newEvent(proto.Instance_REQUESTED, "type-a", "", ""),
	})

	assert.Empty(t, provider.launchCalls)
}

// TestCloudInstanceUpdater_Terminate TERMINATING events collect the cloud
// instance IDs and terminate.
func TestCloudInstanceUpdater_Terminate(t *testing.T) {
	provider := &fakeCloudProvider{}
	updater := NewCloudInstanceUpdater(provider)

	updater.Notify([]*proto.InstanceUpdateEvent{
		newEvent(proto.Instance_TERMINATING, "type-a", "", "cloud-1"),
		newEvent(proto.Instance_TERMINATING, "type-b", "", "cloud-2"),
	})

	assert.Len(t, provider.terminateCalls, 1)
	call := provider.terminateCalls[0]
	assert.ElementsMatch(t, []string{"cloud-1", "cloud-2"}, call.ids)
	assert.NotEmpty(t, call.requestId)
	assert.Empty(t, provider.launchCalls)
}

// TestCloudInstanceUpdater_IgnoresOtherStatus other status events do not
// trigger cloud operations.
func TestCloudInstanceUpdater_IgnoresOtherStatus(t *testing.T) {
	provider := &fakeCloudProvider{}
	updater := NewCloudInstanceUpdater(provider)

	updater.Notify([]*proto.InstanceUpdateEvent{
		newEvent(proto.Instance_QUEUED, "type-a", "req-1", ""),
		newEvent(proto.Instance_RAY_RUNNING, "type-a", "req-1", "cloud-1"),
	})
	updater.Notify(nil)

	assert.Empty(t, provider.launchCalls)
	assert.Empty(t, provider.terminateCalls)
}

// TestCloudInstanceUpdater_ProviderError cloud operation failures only log and
// do not propagate upward.
func TestCloudInstanceUpdater_ProviderError(t *testing.T) {
	provider := &fakeCloudProvider{
		launchErr:    errors.New("launch failed"),
		terminateErr: errors.New("terminate failed"),
	}
	updater := NewCloudInstanceUpdater(provider)

	// Should not panic.
	updater.Notify([]*proto.InstanceUpdateEvent{
		newEvent(proto.Instance_REQUESTED, "type-a", "req-1", ""),
		newEvent(proto.Instance_TERMINATING, "type-a", "", "cloud-1"),
	})
	assert.Len(t, provider.launchCalls, 1)
	assert.Len(t, provider.terminateCalls, 1)
}
