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

package submitter

import (
	"errors"
	"fmt"

	"github.com/ray-project/ray/go/pkg/ids"
	"github.com/ray-project/ray/go/pkg/log"
	"github.com/ray-project/ray/go/proto"
)

// TaskOptions contains options for task submission.
// Corresponds to Java's io.ray.api.options.CallOptions.
type TaskOptions struct {
	// Resources is a map of resource name to quantity required.
	// Example: map[string]float64{"CPU": 2, "memory": 1024}
	Resources map[string]float64

	// NumGPUs specifies the number of GPUs required for this task.
	// Supports fractional GPUs (e.g., 0.5 for half a GPU).
	// This is a convenience field that sets Resources["GPU"].
	NumGPUs float64

	// GPUIDs specifies specific GPU IDs to use (GPU affinity).
	// Format: CUDA_VISIBLE_DEVICES style (e.g., ["0", "2"] for GPUs 0 and 2).
	// If empty, any available GPUs will be used.
	GPUIDs []string

	// PlacementGroup is the placement group for this task.
	PlacementGroup *PlacementGroupOptions

	// RetryPolicy is the retry policy for this task.
	RetryPolicy *RetryPolicy

	// RuntimeEnv is the runtime environment for this task.
	// This is a serialized string representing the runtime environment configuration.
	RuntimeEnv string

	// Name is the optional name for the task.
	// If specified, the task can be identified by name in monitoring and debugging.
	Name string

	// ConcurrencyGroupName is the optional concurrency group name for the task.
	// It only applies to tasks submitted to an actor with concurrency groups.
	// Corresponds to Java's CallOptions.setConcurrencyGroupName.
	ConcurrencyGroupName string
}

// ActorLifetime represents the lifetime of an actor.
// Consistent with Java's io.ray.api.options.ActorLifetime.
// Defined locally (rather than in contract) to avoid a circular dependency:
// contract imports submitter, so submitter cannot import contract.
type ActorLifetime int

const (
	// ActorLifetimeNonDetached means the actor will be terminated when the
	// driver that created it exits. This is the default.
	ActorLifetimeNonDetached ActorLifetime = iota
	// ActorLifetimeDetached means the actor keeps running even after the
	// driver that created it exits.
	ActorLifetimeDetached
)

// ActorCreationOptions contains options for actor creation.
// Corresponds to Java's io.ray.api.options.ActorCreationOptions.
type ActorCreationOptions struct {
	// Resources is a map of resource name to quantity required.
	Resources map[string]float64

	// Name is the optional name for the actor.
	// If specified, the actor can be retrieved by name using GetActor().
	Name string

	// Namespace is the optional namespace for the actor.
	// If not specified, the current namespace is used.
	Namespace string

	// MaxRestarts is the maximum number of times to restart the actor on failure.
	// -1 means unlimited restarts, 0 means no restarts.
	MaxRestarts int

	// MaxTaskRetries is the maximum number of times to retry a task on failure.
	// This applies to tasks submitted to this actor.
	MaxTaskRetries int

	// MaxConcurrency is the maximum number of concurrent calls for the actor.
	// 0 means use the default concurrency, -1 means unlimited.
	MaxConcurrency int

	// PlacementGroup is the placement group for this actor.
	PlacementGroup *PlacementGroupOptions

	// ConcurrencyGroups is the list of concurrency groups for this actor.
	// Each group can have its own maximum concurrent calls limit.
	// Defined locally to avoid circular dependency with api package.
	ConcurrencyGroups []ConcurrencyGroup

	// RuntimeEnv is the runtime environment for this actor.
	RuntimeEnv string

	// Lifetime is the actor lifetime (detached or non-detached).
	// Consistent with Java's ActorCreationOptions.setLifetime.
	Lifetime ActorLifetime

	// IsAsync indicates whether the actor uses async direct call mode.
	// Consistent with Java's ActorCreationOptions.setIsAsync.
	IsAsync bool

	// MaxPendingCalls is the maximum number of pending calls for the actor.
	// -1 means unlimited. Consistent with Java's ActorCreationOptions.setMaxPendingCalls.
	// A zero value (0) means "unset" and is resolved to MaxPendingCallsUnlimited by
	// NormalizeMaxPendingCalls before reaching a backend.
	MaxPendingCalls int
}

// MaxPendingCallsUnlimited is the sentinel value meaning unlimited pending calls.
const MaxPendingCallsUnlimited = -1

// ValidateMaxPendingCallsValue validates a MaxPendingCalls value.
// Valid values are -1 (unlimited), 0 (unset, resolved to unlimited) or any
// positive number. Only values less than -1 are rejected. This is the single
// source of truth for the boundary rule and error message, shared by local_mode,
// the native backend, and api.ActorCreationOptions.Validate().
func ValidateMaxPendingCallsValue(maxPendingCalls int) error {
	if maxPendingCalls < MaxPendingCallsUnlimited {
		return fmt.Errorf("maxPendingCalls must be -1 (unlimited), 0 (unset) or a positive value, got %d", maxPendingCalls)
	}
	return nil
}

// ValidateMaxPendingCalls validates the MaxPendingCalls field.
func (o *ActorCreationOptions) ValidateMaxPendingCalls() error {
	return ValidateMaxPendingCallsValue(o.MaxPendingCalls)
}

// NormalizeMaxPendingCalls resolves the "unset" zero value to the unlimited
// sentinel so a zero-value ActorCreationOptions is valid and behaves as
// unlimited, consistent with the builder default. It must be called by backends
// after validation and before the value is used.
func (o *ActorCreationOptions) NormalizeMaxPendingCalls() {
	if o.MaxPendingCalls == 0 {
		o.MaxPendingCalls = MaxPendingCallsUnlimited
		log.Log.Info("maxPendingCalls=0 treated as unlimited (unset)",
			"maxPendingCalls", o.MaxPendingCalls)
	}
}

// ConcurrencyGroup represents a concurrency group for an actor.
// This is a duplicate of api.ConcurrencyGroup to avoid circular dependency.
// Changes to the concurrency group structure must be synchronized between packages.
type ConcurrencyGroup struct {
	// Name is the unique name of the concurrency group
	Name string

	// MaxCalls is the maximum number of concurrent calls for methods in this group.
	// 0 means use the actor's MaxConcurrency setting.
	// -1 means unlimited.
	MaxCalls int

	// Methods is the list of method names belonging to this group.
	// An empty list indicates this group contains all unassigned methods (default group).
	Methods []string
}

// PlacementGroupOptions contains options for placement group.
type PlacementGroupOptions struct {
	// ID is the placement group ID.
	ID ids.PlacementGroupID

	// BundleIndex is the index of the bundle to use.
	// Placement groups divide resources into bundles, and this specifies which bundle to use.
	BundleIndex int
}

// RetryPolicy contains retry policy for tasks.
type RetryPolicy struct {
	// MaxRetries is the maximum number of retries.
	// -1 means unlimited retries, 0 means no retries.
	MaxRetries int
}

// WithGPUs sets the GPU resources for a task.
// This is a convenience function that sets both NumGPUs and optionally GPUIDs.
//
// Parameters:
//   - numGPUs: Number of GPUs required (supports fractional, e.g., 0.5)
//   - gpuIDs: Optional specific GPU IDs to use (GPU affinity)
//
// Returns:
//   - func(*TaskOptions): Option function to configure TaskOptions
//
// Example:
//
//	ray.Remote(func() {...}, submitter.WithGPUs(1.0))           // 1 GPU
//	ray.Remote(func() {...}, submitter.WithGPUs(0.5))           // 0.5 GPU
//	ray.Remote(func() {...}, submitter.WithGPUs(1.0, "0", "2")) // GPUs 0 and 2
func WithGPUs(numGPUs float64, gpuIDs ...string) func(*TaskOptions) {
	return func(opts *TaskOptions) {
		opts.NumGPUs = numGPUs
		if numGPUs > 0 {
			// Set Resources["GPU"] for backward compatibility
			if opts.Resources == nil {
				opts.Resources = make(map[string]float64)
			}
			opts.Resources["GPU"] = numGPUs
		}
		if len(gpuIDs) > 0 {
			opts.GPUIDs = gpuIDs
		}
	}
}

// WithResources sets custom resources for a task.
// This allows specifying any named resource (e.g., "TPU", "memory", "custom_resource").
//
// Parameters:
//   - resources: Map of resource name to quantity required
//
// Returns:
//   - func(*TaskOptions): Option function to configure TaskOptions
//
// Example:
//
//	ray.Remote(func() {...}, submitter.WithResources(map[string]float64{
//	    "TPU": 2.0,
//	    "memory": 1024.0,
//	}))
func WithResources(resources map[string]float64) func(*TaskOptions) {
	return func(opts *TaskOptions) {
		opts.Resources = resources
	}
}

// PlacementGroupCreationOptions holds the parameters used to create a
// placement group from Go.
type PlacementGroupCreationOptions struct {
	// Name is the optional unique name of the placement group.
	Name string

	// Bundles is the list of resource bundles that make up the placement group.
	// Each bundle is a map of resource name to quantity.
	Bundles []map[string]float64

	// Strategy is the placement strategy: 0=PACK, 1=SPREAD, 2=STRICT_PACK,
	// 3=STRICT_SPREAD. Consistent with rpc.PlacementStrategy.
	Strategy int32
}

// Validate checks that required fields are set and strategy is valid.
//
// The name is optional: anonymous placement groups are legal (consistent with
// the Python and Java APIs, which treat the name as optional). Only the bundle
// list and the strategy are required.
func (o PlacementGroupCreationOptions) Validate() error {
	if len(o.Bundles) == 0 {
		return errors.New("at least one bundle is required")
	}
	switch o.Strategy {
	case int32(proto.PlacementStrategy_PACK),
		int32(proto.PlacementStrategy_SPREAD),
		int32(proto.PlacementStrategy_STRICT_PACK),
		int32(proto.PlacementStrategy_STRICT_SPREAD):
	default:
		return fmt.Errorf("invalid placement strategy %d", o.Strategy)
	}
	return nil
}
