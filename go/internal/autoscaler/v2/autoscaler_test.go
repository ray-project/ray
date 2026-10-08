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

package v2

import (
	"context"
	"errors"
	"testing"

	"github.com/ray-project/ray/go/internal/autoscaler"
	"github.com/ray-project/ray/go/internal/autoscaler/v2/instance_manager"
	"github.com/ray-project/ray/go/internal/autoscaler/v2/instance_manager/cloud_providers"
	"github.com/ray-project/ray/go/pkg/gcs"
	"github.com/ray-project/ray/go/proto"
	"github.com/stretchr/testify/assert"
)

// ---------- Test fakes ----------

// fakeGcsClient is a fake GCS client that only overrides GetAutoscalerStatus and Address.
type fakeGcsClient struct {
	gcs.Client // Embedded interface; the unoverridden methods panic when called.
	reply      *proto.GetClusterStatusReply
	err        error
	address    string
}

func (f *fakeGcsClient) GetAutoscalerStatus(ctx context.Context) (*proto.GetClusterStatusReply, error) {
	return f.reply, f.err
}

func (f *fakeGcsClient) Address() string {
	return f.address
}

// fakeConfigReader is a fake config reader.
type fakeConfigReader struct {
	refreshErr error
	config     *instance_manager.AutoscalingConfig
	configErr  error
}

func (f *fakeConfigReader) RefreshCachedAutoscalingConfig() error {
	return f.refreshErr
}

func (f *fakeConfigReader) GetCachedAutoscalingConfig() (*instance_manager.AutoscalingConfig, error) {
	return f.config, f.configErr
}

// fakeCloudProvider is a fake cloud instance provider.
type fakeCloudProvider struct {
	instances map[string]autoscaler.CloudInstance
	err       error
	errors    []error
}

func (f *fakeCloudProvider) GetNonTerminated() (map[string]autoscaler.CloudInstance, error) {
	return f.instances, f.err
}

func (f *fakeCloudProvider) Terminate(ids []string, requestId string) error {
	return nil
}

func (f *fakeCloudProvider) Launch(shape map[string]int, requestId string) error {
	return nil
}

func (f *fakeCloudProvider) PollErrors() []error {
	return f.errors
}

// fakeReconciler is a fake reconciler that records the reconciliation arguments it received.
type fakeReconciler struct {
	called      bool
	args        *autoscaler.ReconcileArgs
	returnState *proto.AutoscalingState
	err         error
}

func (f *fakeReconciler) Reconcile(args *autoscaler.ReconcileArgs) (*proto.AutoscalingState, error) {
	f.called = true
	f.args = args
	return f.returnState, f.err
}

// newAutoscalerForTest injects the fake dependencies through the low-level
// constructor and returns the injected error queues (simulating the external
// references held by the subscribers). The reconciler parameter is an interface
// type: passing nil (reconciler not wired yet) is not mistakenly wrapped into a
// non-nil interface.
func newAutoscalerForTest(
	gcsClient *fakeGcsClient,
	configReader *fakeConfigReader,
	cloudProvider *fakeCloudProvider,
	reconciler IReconciler,
) (*Autoscaler, chan error, chan error) {
	rayStopErrorsQueue := make(chan error, ErrorQueueCapacity)
	rayInstallErrorsQueue := make(chan error, ErrorQueueCapacity)
	autoscaler, err := newAutoscaler(gcsClient, configReader, cloudProvider, reconciler,
		rayStopErrorsQueue, rayInstallErrorsQueue, nil, nil)
	if err != nil {
		panic(err)
	}
	return autoscaler, rayStopErrorsQueue, rayInstallErrorsQueue
}

// newFakeGcsReply builds a GCS reply carrying a cluster resource state.
func newFakeGcsReply(version int64, nodeStates ...*proto.NodeState) *fakeGcsClient {
	return &fakeGcsClient{
		reply: &proto.GetClusterStatusReply{
			ClusterResourceState: &proto.ClusterResourceState{
				ClusterResourceStateVersion: version,
				NodeStates:                  nodeStates,
			},
		},
	}
}

// ---------- UpdateAutoscalingState ----------

// TestUpdateAutoscalingState_ReconcileSuccess is the happy path: the error queues
// are drained, the cluster state / config / cloud instances are collected and
// passed to the reconciler, and the reconciliation result is returned as-is.
func TestUpdateAutoscalingState_ReconcileSuccess(t *testing.T) {
	gcsClient := newFakeGcsReply(42)
	configReader := &fakeConfigReader{config: &instance_manager.AutoscalingConfig{}}
	cloudProvider := &fakeCloudProvider{
		instances: map[string]autoscaler.CloudInstance{
			"instance-1": {CloudInstanceId: "instance-1", NodeKind: proto.NodeKind_WORKER},
		},
		errors: []error{errors.New("provider error")},
	}
	expectedState := &proto.AutoscalingState{AutoscalerStateVersion: 7}
	reconciler := &fakeReconciler{returnState: expectedState}

	autoscaler, stopQueue, installQueue := newAutoscalerForTest(gcsClient, configReader, cloudProvider, reconciler)
	// Pre-fill the error queues through the external references (simulating the
	// RayStopper/ThreadedRayInstaller subscribers).
	stopQueue <- errors.New("stop error 1")
	stopQueue <- errors.New("stop error 2")
	installQueue <- errors.New("install error 1")

	state, err := autoscaler.UpdateAutoscalingState()

	assert.NoError(t, err)
	assert.Equal(t, expectedState, state)

	// The reconciler was called and every input was passed through correctly.
	assert.True(t, reconciler.called)
	args := reconciler.args
	assert.Equal(t, int64(42), args.RayClusterResourceState.ClusterResourceStateVersion)
	assert.Len(t, args.RayStopErrors, 2)
	assert.Len(t, args.RayInstallErrors, 1)
	assert.Len(t, args.CloudProviderErrors, 1)
	assert.Equal(t, cloudProvider.instances, args.NonTerminatedCloudInstances)
	assert.Equal(t, configReader.config, args.AutoscalingConfig)

	// The injected queues were drained (subscribers and the Autoscaler share the
	// same reference).
	assert.Empty(t, drainErrors(stopQueue))
	assert.Empty(t, drainErrors(installQueue))
}

// TestUpdateAutoscalingState_WrongClusterIDIsFatal treats the WrongClusterID error
// as fatal: an error is returned (the monitor loop terminates the process because
// of it) instead of skipping the round.
func TestUpdateAutoscalingState_WrongClusterIDIsFatal(t *testing.T) {
	reconciler := &fakeReconciler{err: errors.New("AuthenticationError: WrongClusterID abc")}
	autoscaler, _, _ := newAutoscalerForTest(
		newFakeGcsReply(1), &fakeConfigReader{config: &instance_manager.AutoscalingConfig{}}, &fakeCloudProvider{}, reconciler)

	state, err := autoscaler.UpdateAutoscalingState()

	assert.Nil(t, state)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "WrongClusterID")
}

// TestUpdateAutoscalingState_ReconcilerErrorSkipped treats a generic reconciler
// error as non-fatal: only log and return (nil, nil); the monitor loop keeps running.
func TestUpdateAutoscalingState_ReconcilerErrorSkipped(t *testing.T) {
	reconciler := &fakeReconciler{err: errors.New("some reconcile failure")}
	autoscaler, _, _ := newAutoscalerForTest(
		newFakeGcsReply(1), &fakeConfigReader{config: &instance_manager.AutoscalingConfig{}}, &fakeCloudProvider{}, reconciler)

	state, err := autoscaler.UpdateAutoscalingState()

	assert.NoError(t, err)
	assert.Nil(t, state)
	assert.True(t, reconciler.called)
}

// TestUpdateAutoscalingState_GcsErrorSkipped covers a failed GCS cluster resource
// state fetch: returns (nil, nil) and no reconciliation is triggered.
func TestUpdateAutoscalingState_GcsErrorSkipped(t *testing.T) {
	reconciler := &fakeReconciler{returnState: &proto.AutoscalingState{}}
	autoscaler, _, _ := newAutoscalerForTest(
		&fakeGcsClient{err: errors.New("gcs unavailable")},
		&fakeConfigReader{config: &instance_manager.AutoscalingConfig{}}, &fakeCloudProvider{}, reconciler)

	state, err := autoscaler.UpdateAutoscalingState()

	assert.NoError(t, err)
	assert.Nil(t, state)
	assert.False(t, reconciler.called)
}

// TestUpdateAutoscalingState_ConfigRefreshErrorSkipped covers a failed config
// refresh: returns (nil, nil) and no reconciliation is triggered.
func TestUpdateAutoscalingState_ConfigRefreshErrorSkipped(t *testing.T) {
	reconciler := &fakeReconciler{returnState: &proto.AutoscalingState{}}
	autoscaler, _, _ := newAutoscalerForTest(
		newFakeGcsReply(1),
		&fakeConfigReader{refreshErr: errors.New("config file gone")},
		&fakeCloudProvider{}, reconciler)

	state, err := autoscaler.UpdateAutoscalingState()

	assert.NoError(t, err)
	assert.Nil(t, state)
	assert.False(t, reconciler.called)
}

// TestNewAutoscaler_NilQueueRejected covers construction with a nil error queue:
// it must fail because the queues must be created and injected by the assembly
// point so subscribers and the Autoscaler share the same reference.
func TestNewAutoscaler_NilQueueRejected(t *testing.T) {
	_, err := newAutoscaler(newFakeGcsReply(1), &fakeConfigReader{config: &instance_manager.AutoscalingConfig{}},
		&fakeCloudProvider{}, nil, nil, nil, nil, nil)
	assert.Error(t, err)

	_, err = newAutoscaler(newFakeGcsReply(1), &fakeConfigReader{config: &instance_manager.AutoscalingConfig{}},
		&fakeCloudProvider{}, nil, make(chan error, 1), nil, nil, nil)
	assert.Error(t, err)
}

// TestUpdateAutoscalingState_NilReconcilerSkipped covers a not-yet-wired (nil)
// reconciler: every round is skipped and returns (nil, nil), the monitor loop
// keeps running.
func TestUpdateAutoscalingState_NilReconcilerSkipped(t *testing.T) {
	autoscaler, _, _ := newAutoscalerForTest(
		newFakeGcsReply(1), &fakeConfigReader{config: &instance_manager.AutoscalingConfig{}}, &fakeCloudProvider{}, nil)

	state, err := autoscaler.UpdateAutoscalingState()

	assert.NoError(t, err)
	assert.Nil(t, state)
}

// ---------- provider selection of initCloudInstanceProvider ----------

// newProviderConfigConfig builds an autoscaling config with the given provider type.
func newProviderConfigConfig(providerType string) *instance_manager.AutoscalingConfig {
	return &instance_manager.AutoscalingConfig{
		Configs: map[string]interface{}{
			"provider": map[string]interface{}{"type": providerType},
		},
	}
}

// TestInitCloudInstanceProvider_Readonly expects ReadOnlyProvider for the readonly type.
func TestInitCloudInstanceProvider_Readonly(t *testing.T) {
	provider, err := initCloudInstanceProvider(newProviderConfigConfig("readonly"), nil)

	assert.NoError(t, err)
	assert.IsType(t, &cloud_providers.ReadOnlyProvider{}, provider)
}

// TestInitCloudInstanceProvider_KubeRayNotImplemented expects the kuberay branch
// entry to be preserved and return an error for now.
func TestInitCloudInstanceProvider_KubeRayNotImplemented(t *testing.T) {
	_, err := initCloudInstanceProvider(newProviderConfigConfig("kuberay"), nil)

	assert.Error(t, err)
	assert.Contains(t, err.Error(), "kuberay provider not implemented")
}

// TestInitCloudInstanceProvider_Local expects LocalProvider for the local type,
// with the head state already recorded (head_ip visible as a running instance).
func TestInitCloudInstanceProvider_Local(t *testing.T) {
	t.Setenv("RAY_TMPDIR", t.TempDir())
	config := &instance_manager.AutoscalingConfig{
		Configs: map[string]interface{}{
			"cluster_name": "test-cluster",
			"provider": map[string]interface{}{
				"type":       "local",
				"head_ip":    "127.0.0.1",
				"worker_ips": []interface{}{"127.0.0.2"},
			},
		},
	}

	provider, err := initCloudInstanceProvider(config, nil)

	assert.NoError(t, err)
	assert.IsType(t, &cloud_providers.LocalProvider{}, provider)

	instances, err := provider.GetNonTerminated()
	assert.NoError(t, err)
	assert.Contains(t, instances, "127.0.0.1")
}

// TestInitCloudInstanceProvider_Unsupported expects cloud vendor types to be unsupported.
func TestInitCloudInstanceProvider_Unsupported(t *testing.T) {
	_, err := initCloudInstanceProvider(newProviderConfigConfig("aws"), nil)

	assert.Error(t, err)
	assert.Contains(t, err.Error(), "unsupported provider type")
}

// TestInitInstanceManager assembles the instance manager and its subscribers;
// every product must be non-nil.
func TestInitInstanceManager(t *testing.T) {
	instanceManager, cloudResourceMonitor, rayStopper, rayInstaller := initInstanceManager(
		"test-session", &fakeCloudProvider{}, newFakeGcsReply(1),
		newProviderConfigConfig("readonly"), make(chan error, ErrorQueueCapacity), make(chan error, ErrorQueueCapacity))

	assert.NotNil(t, instanceManager)
	assert.NotNil(t, cloudResourceMonitor)
	assert.NotNil(t, rayStopper)
	// The readonly provider does not register the Ray-install subscriber.
	assert.Nil(t, rayInstaller)
}

// TestInitInstanceManager_LocalRegistersInstaller expects the local provider to
// register the Ray-install subscriber.
func TestInitInstanceManager_LocalRegistersInstaller(t *testing.T) {
	gcsClient := newFakeGcsReply(1)
	gcsClient.address = "127.0.0.1:6379"

	_, _, _, rayInstaller := initInstanceManager(
		"test-session", &fakeCloudProvider{}, gcsClient,
		newProviderConfigConfig("local"), make(chan error, ErrorQueueCapacity), make(chan error, ErrorQueueCapacity))

	assert.NotNil(t, rayInstaller)
}

// TestInitInstanceManager_DisableNodeUpdaters expects no Ray-install subscriber
// when node updaters are disabled.
func TestInitInstanceManager_DisableNodeUpdaters(t *testing.T) {
	config := &instance_manager.AutoscalingConfig{
		Configs: map[string]interface{}{
			"provider": map[string]interface{}{
				"type":                  "local",
				"disable_node_updaters": true,
			},
		},
	}

	_, _, _, rayInstaller := initInstanceManager(
		"test-session", &fakeCloudProvider{}, newFakeGcsReply(1),
		config, make(chan error, ErrorQueueCapacity), make(chan error, ErrorQueueCapacity))

	assert.Nil(t, rayInstaller)
}

// TestHeadNodeIPFromGcsAddress extracts the head node IP from a GCS address.
func TestHeadNodeIPFromGcsAddress(t *testing.T) {
	assert.Equal(t, "127.0.0.1", headNodeIPFromGcsAddress("127.0.0.1:6379"))
	assert.Equal(t, "10.0.0.1", headNodeIPFromGcsAddress("10.0.0.1"))
	assert.Equal(t, "::1", headNodeIPFromGcsAddress("[::1]:6379"))
}
