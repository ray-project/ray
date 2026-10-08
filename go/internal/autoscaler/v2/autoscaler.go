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
	"fmt"
	"net"
	"strings"
	"time"

	"github.com/ray-project/ray/go/internal/autoscaler"
	"github.com/ray-project/ray/go/internal/autoscaler/v2/instance_manager"
	"github.com/ray-project/ray/go/internal/autoscaler/v2/instance_manager/cloud_providers"
	"github.com/ray-project/ray/go/internal/autoscaler/v2/instance_manager/reconciler"
	"github.com/ray-project/ray/go/internal/autoscaler/v2/instance_manager/subscribers"
	"github.com/ray-project/ray/go/internal/autoscaler/v2/scheduler"
	"github.com/ray-project/ray/go/pkg/gcs"
	"github.com/ray-project/ray/go/pkg/log"
	"github.com/ray-project/ray/go/proto"
)

// IReconciler is the reconciler interface.
// It corresponds to the Python side instance_manager.reconciler.Reconciler.reconcile.
// The full reconciliation logic (passive sync of instance state against the
// cloud provider / Ray cluster state, proactive scaling state transitions,
// metrics reporting) is provided by the reconciler package and injected
// through this interface.
type IReconciler interface {
	// Reconcile runs one reconciliation round and returns the autoscaling
	// state of that round.
	Reconcile(args *autoscaler.ReconcileArgs) (*proto.AutoscalingState, error)
}

// ErrorQueueCapacity is the capacity of the error queues.
// The Python side uses unbounded Queues; the Go side implements them as
// buffered channels. The queues are created and injected by NewAutoscaler:
// subscribers (RayStopper, ThreadedRayInstaller) write into them and
// UpdateAutoscalingState drains them each round; both sides must hold the
// same reference.
const ErrorQueueCapacity = 100

// clusterResourceStateTimeoutS is the timeout (in seconds) for fetching the
// cluster resource state.
// Corresponds to the Python side sdk.DEFAULT_RPC_TIMEOUT_S.
const clusterResourceStateTimeoutS = 10 * time.Second

// Autoscaler is the core autoscaler component.
// It corresponds to the Autoscaler class of python/autoscaler/v2/autoscaler.py:
// the constructor assembles all the long-lived collaborators (cloud provider /
// instance manager and its subscribers / scheduler / reconciler), and each round
// UpdateAutoscalingState collects the current cluster state and drives one
// reconciliation.
type Autoscaler struct {
	gcsClient             gcs.Client
	configReader          instance_manager.IConfigReader
	cloudProvider         autoscaler.ICloudInstanceProvider
	reconciler            IReconciler
	rayStopper            *subscribers.RayStopper           // Ray-stop subscriber, created and registered by NewAutoscaler
	rayInstaller          *subscribers.ThreadedRayInstaller // Ray-install subscriber, created and registered by NewAutoscaler
	rayStopErrorsQueue    chan error
	rayInstallErrorsQueue chan error
}

// NewAutoscaler assembles all the long-lived collaborators and creates the Autoscaler.
// It corresponds to the Python side Autoscaler.__init__ + _init_cloud_instance_provider +
// _init_instance_manager: the error queues, cloud provider, instance manager and its
// subscribers, scheduler and reconciler are all created here (unlike the Python static
// Reconciler, the Go reconciler is injected at construction).
func NewAutoscaler(
	gcsClient gcs.Client,
	configReader instance_manager.IConfigReader,
	sessionName string,
	eventLogger scheduler.EventLogger,
	metricsReporter reconciler.InstanceMetricsReporter,
) (*Autoscaler, error) {
	// Read the cached autoscaling config to pick the cloud provider type
	// (corresponds to config_reader.get_cached_autoscaling_config() in the
	// Python _init_cloud_instance_provider). Note that only the cached value is
	// read here, no refresh: the refresh happens in every round of
	// updateAutoscalingState (corresponding to Python update_autoscaling_state);
	// at assembly time the read-only provider type is fixed, so there is no need
	// to reach GCS upfront.
	config, err := configReader.GetCachedAutoscalingConfig()
	if err != nil {
		return nil, fmt.Errorf("failed to get cached autoscaling config: %w", err)
	}
	log.Log.V(1).Info("Using Autoscaling Config", "provider", config.GetProvider())

	// Create the cloud instance provider (the provider selection of the Python
	// _init_cloud_instance_provider).
	cloudProvider, err := initCloudInstanceProvider(config, gcsClient)
	if err != nil {
		return nil, fmt.Errorf("failed to init cloud instance provider: %w", err)
	}

	// Create the subscriber error queues (written by RayStopper/ThreadedRayInstaller,
	// drained by the Autoscaler every round). Corresponds to the Python __init__
	// fields _ray_stop_errors_queue/_ray_install_errors_queue; the queues are
	// created here so the subscribers wired below share the same reference as
	// the Autoscaler.
	rayStopErrorsQueue := make(chan error, ErrorQueueCapacity)
	rayInstallErrorsQueue := make(chan error, ErrorQueueCapacity)

	// Assemble the instance manager and its subscribers (corresponds to the
	// Python _init_instance_manager).
	instanceManager, cloudResourceMonitor, rayStopper, rayInstaller := initInstanceManager(
		sessionName, cloudProvider, gcsClient, config,
		rayStopErrorsQueue, rayInstallErrorsQueue)

	// Create the scheduler (when eventLogger is nil the scheduler skips logging).
	sched := scheduler.NewResourceDemandScheduler(eventLogger)

	// Create the reconciler, injecting all the long-lived collaborators and the
	// metrics reporter.
	r := reconciler.NewReconciler(
		instanceManager, sched, cloudProvider, cloudResourceMonitor, metricsReporter)

	return newAutoscaler(gcsClient, configReader, cloudProvider, r,
		rayStopErrorsQueue, rayInstallErrorsQueue, rayStopper, rayInstaller)
}

// newAutoscaler is the low-level constructor: it injects the collaborators that
// were already built. NewAutoscaler finishes through this function; tests that
// replace cloudProvider/reconciler with fakes also construct through it. The
// error queues are created and injected by the caller: subscribers write into
// them and UpdateAutoscalingState drains them each round, so both sides must
// hold the same reference.
func newAutoscaler(
	gcsClient gcs.Client,
	configReader instance_manager.IConfigReader,
	cloudProvider autoscaler.ICloudInstanceProvider,
	reconciler IReconciler,
	rayStopErrorsQueue chan error,
	rayInstallErrorsQueue chan error,
	rayStopper *subscribers.RayStopper,
	rayInstaller *subscribers.ThreadedRayInstaller,
) (*Autoscaler, error) {
	if rayStopErrorsQueue == nil || rayInstallErrorsQueue == nil {
		return nil, fmt.Errorf("rayStopErrorsQueue and rayInstallErrorsQueue must not be nil")
	}
	return &Autoscaler{
		gcsClient:             gcsClient,
		configReader:          configReader,
		cloudProvider:         cloudProvider,
		reconciler:            reconciler,
		rayStopper:            rayStopper,
		rayInstaller:          rayInstaller,
		rayStopErrorsQueue:    rayStopErrorsQueue,
		rayInstallErrorsQueue: rayInstallErrorsQueue,
	}, nil
}

// initCloudInstanceProvider creates the cloud instance provider based on the
// provider type of the autoscaling config.
// It corresponds to the provider selection of the Python side
// Autoscaler._init_cloud_instance_provider:
//   - readonly: ReadOnlyProvider, using the Ray node state reported to GCS as the
//     cloud instances
//   - kuberay:  KubeRayProvider (to be implemented, see
//     go/docs/autoscaler_v2_cloud_provider_implementation.md)
//   - local:    LocalProvider, fixed-IP nodes managed through a local JSON state file
//   - others (aws/gcp and other cloud vendors): unsupported, returns an error
func initCloudInstanceProvider(
	config *instance_manager.AutoscalingConfig,
	gcsClient gcs.Client,
) (autoscaler.ICloudInstanceProvider, error) {
	switch config.GetProvider() {
	case instance_manager.ProviderKubeRay:
		// TODO: port the KubeRayProvider (patching the workerGroupSpecs.replicas
		//       and scaleStrategy.workersToDelete of the RayCluster CRD through
		//       the K8s API).
		return nil, fmt.Errorf("kuberay provider not implemented yet")

	case instance_manager.ProviderReadOnly:
		// The read-only provider is fully implemented; it suits local private Ray
		// clusters (no cloud vendor API).
		return cloud_providers.NewReadOnlyProvider(gcsClient), nil

	case instance_manager.ProviderLocal:
		// Local provider: fixed-IP nodes (head_ip + worker_ips) managed through a
		// local JSON state file; it implements ICloudInstanceProvider natively
		// (not through the Python-side NodeProviderAdapter).
		providerConfig := autoscaler.GetValueWithDefault[map[string]interface{}](
			config.Configs, map[string]interface{}{}, "provider")
		clusterName, _ := config.GetConfig("cluster_name", "").(string)
		localProvider, err := cloud_providers.NewLocalProvider(clusterName, providerConfig)
		if err != nil {
			return nil, fmt.Errorf("failed to create local provider: %w", err)
		}
		// Mirror Python record_local_head_state_if_needed: the head state is
		// recorded by `ray up` on the machine that issued it, so the head running
		// the autoscaler must record head_ip as running itself.
		if err := localProvider.RecordLocalHeadStateIfNeeded(); err != nil {
			return nil, fmt.Errorf("failed to record local head state: %w", err)
		}
		return localProvider, nil

	default:
		// Cloud vendor providers (aws/gcp/azure/aliyun etc.) are out of scope.
		return nil, fmt.Errorf("unsupported provider type: %v", config.GetProvider())
	}
}

// initInstanceManager creates the instance manager and all its subscribers.
// It corresponds to the Python side Autoscaler._init_instance_manager: instance
// storage -> subscriber list (CloudInstanceUpdater/RayStopper/ThreadedRayInstaller/
// CloudResourceMonitor) -> InstanceManager.
// It returns the instance manager, the cloud resource monitor, the Ray-stop
// subscriber and the Ray-install subscriber (nil when not registered) that
// NewAutoscaler needs to finish the assembly.
// The error queues are created and injected by NewAutoscaler;
// RayStopper/ThreadedRayInstaller report Ray stop/install failures through them;
// for the GCS DrainNodes call of the Ray stop path see the TODO in ray_stopper.go.
func initInstanceManager(
	sessionName string,
	cloudProvider autoscaler.ICloudInstanceProvider,
	gcsClient gcs.Client,
	config *instance_manager.AutoscalingConfig,
	rayStopErrorsQueue chan error,
	rayInstallErrorsQueue chan error,
) (*instance_manager.InstanceManager, *subscribers.CloudResourceMonitor, *subscribers.RayStopper, *subscribers.ThreadedRayInstaller) {
	instanceStorage := instance_manager.NewInstanceStorage(
		sessionName, instance_manager.NewInMemoryStorage())
	cloudInstanceUpdater := subscribers.NewCloudInstanceUpdater(cloudProvider)
	rayStopper := subscribers.NewRayStopper(gcsClient, rayStopErrorsQueue)
	instanceStatusUpdateSubscribers := []instance_manager.InstanceUpdatedSubscriber{
		cloudInstanceUpdater,
		rayStopper,
	}

	// Register the Ray-install subscriber (corresponds to the Python
	// `not config.disable_node_updaters() and isinstance(cloud_provider,
	// NodeProviderAdapter)`. The Go side has no NodeProviderAdapter layer, so the
	// local provider is the only matching scenario; readonly/kuberay do not
	// register it on the Python side either).
	// It sshes into newly allocated nodes to run `ray start`; without it scaled-up
	// instances would stay stuck in RAY_INSTALLING and be judged
	// RAY_INSTALL_FAILED (reclaimed) after the timeout (RayInstallStatusTimeoutS,
	// 30 minutes by default).
	var rayInstaller *subscribers.ThreadedRayInstaller
	if config.GetProvider() == instance_manager.ProviderLocal && !config.DisableNodeUpdaters() {
		// The head node IP is parsed from the GCS address (corresponds to the
		// Python urlsplit("//" + address).hostname).
		headNodeIP := headNodeIPFromGcsAddress(gcsClient.Address())
		// The concurrency is the max number of worker nodes, 50 by default when
		// unset (corresponds to the Python config.get_max_num_worker_nodes() or 50).
		installerOptions := subscribers.InstallerOptions{}
		if maxNumWorkerNodes := config.GetMaxNumWorkerNodes(); maxNumWorkerNodes != nil {
			installerOptions.MaxConcurrentInstalls = int(*maxNumWorkerNodes)
		}
		rayInstaller = subscribers.NewThreadedRayInstaller(
			headNodeIP,
			instanceStorage,
			subscribers.NewSSHRayInstaller(config),
			rayInstallErrorsQueue,
			installerOptions,
		)
		instanceStatusUpdateSubscribers = append(instanceStatusUpdateSubscribers, rayInstaller)
	}

	cloudResourceMonitor := subscribers.NewCloudResourceMonitor()
	instanceStatusUpdateSubscribers = append(instanceStatusUpdateSubscribers, cloudResourceMonitor)
	instanceManager := instance_manager.NewInstanceManager(instanceStorage, instanceStatusUpdateSubscribers)
	return instanceManager, cloudResourceMonitor, rayStopper, rayInstaller
}

// headNodeIPFromGcsAddress extracts the head node IP from the GCS address (host:port).
// Corresponds to the Python urlsplit("//" + gcs_client.address).hostname.
func headNodeIPFromGcsAddress(address string) string {
	if host, _, err := net.SplitHostPort(address); err == nil {
		return host
	}
	return address
}

// UpdateAutoscalingState runs one reconcile over the cluster and returns the new
// autoscaling state.
// It corresponds to the Python Autoscaler.update_autoscaling_state and runs each round:
//  1. drain the error queues reported by the RayStopper / ThreadedRayInstaller
//     subscribers;
//  2. fetch the current Ray cluster resource state from GCS;
//  3. refresh and read the autoscaling config;
//  4. fetch the non-terminated cloud instances and the cloud provider errors, and
//     let the reconciler complete one reconciliation round.
//
// Error semantics (identical to the Python side, expressed through return values here):
//   - an error is returned only when the cluster ID no longer matches
//     (WrongClusterID, the cluster was recreated and this is unrecoverable),
//     corresponding to the Python SystemExit; upon receiving an error the monitor
//     loop terminates the process;
//   - any other error is only logged and turns into (nil, nil), corresponding to
//     the Python None return: the monitor loop skips this round and retries the
//     next one, so the autoscaler does not exit because of it.
func (a *Autoscaler) UpdateAutoscalingState() (*proto.AutoscalingState, error) {
	// A nil reconciler only happens when tests inject a fake; the production path
	// assembles a non-nil reconciler in NewAutoscaler.
	if a.reconciler == nil {
		log.Log.Info("Reconciler is not available yet, skipping autoscaling state update.")
		return nil, nil
	}

	state, err := a.updateAutoscalingState()
	if err != nil {
		if strings.Contains(err.Error(), "WrongClusterID") {
			// The cluster was recreated and the autoscaler cannot recover; return
			// the fatal error upwards to terminate the monitor loop.
			return nil, fmt.Errorf("exiting due to authentication error with WrongClusterID: %w", err)
		}
		// Other errors are only logged; skip this round and retry the next one.
		log.Log.Error(err, "Failed to update autoscaling state.")
		return nil, nil
	}
	return state, nil
}

// updateAutoscalingState runs one orchestration round and returns an error as
// soon as any step fails; UpdateAutoscalingState applies the error semantics
// uniformly.
func (a *Autoscaler) updateAutoscalingState() (*proto.AutoscalingState, error) {
	// Drain the error queue reported by RayStopper (Ray stop failures etc.).
	rayStopErrors := drainErrors(a.rayStopErrorsQueue)

	// Drain the error queue reported by ThreadedRayInstaller (Ray install
	// failures on new cloud machines etc.).
	rayInstallErrors := drainErrors(a.rayInstallErrorsQueue)

	// Fetch the current Ray cluster resource state.
	ctx, cancel := context.WithTimeout(context.Background(), clusterResourceStateTimeoutS)
	defer cancel()
	reply, err := a.gcsClient.GetAutoscalerStatus(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to get cluster resource state from GCS: %w", err)
	}
	if reply == nil || reply.ClusterResourceState == nil {
		return nil, fmt.Errorf("no cluster resource state available from GCS")
	}
	rayClusterResourceState := reply.ClusterResourceState

	// Refresh and read the cached autoscaling config.
	if err := a.configReader.RefreshCachedAutoscalingConfig(); err != nil {
		return nil, fmt.Errorf("failed to refresh autoscaling config: %w", err)
	}
	autoscalingConfig, err := a.configReader.GetCachedAutoscalingConfig()
	if err != nil {
		return nil, fmt.Errorf("failed to get cached autoscaling config: %w", err)
	}

	// Fetch the non-terminated cloud instances and the asynchronous errors
	// accumulated by the cloud provider.
	nonTerminatedCloudInstances, err := a.cloudProvider.GetNonTerminated()
	if err != nil {
		return nil, fmt.Errorf("failed to get non-terminated cloud instances: %w", err)
	}
	cloudProviderErrors := a.cloudProvider.PollErrors()

	// Run one reconciliation: compare the actual state with the desired state,
	// update the instance manager and produce the scaling state.
	return a.reconciler.Reconcile(&autoscaler.ReconcileArgs{
		RayClusterResourceState:     rayClusterResourceState,
		NonTerminatedCloudInstances: nonTerminatedCloudInstances,
		CloudProviderErrors:         cloudProviderErrors,
		RayInstallErrors:            rayInstallErrors,
		RayStopErrors:               rayStopErrors,
		AutoscalingConfig:           autoscalingConfig,
	})
}

// drainErrors drains an error queue without blocking and returns all drained errors.
func drainErrors(queue chan error) []error {
	errors := make([]error, 0)
	for {
		select {
		case e := <-queue:
			errors = append(errors, e)
		default:
			return errors
		}
	}
}
