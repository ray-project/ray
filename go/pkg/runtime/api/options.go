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

package api

import (
	"fmt"

	"github.com/ray-project/ray/go/pkg/runtime/submitter"
)

// Predefined resource type constants for common resource requirements.
// Using these constants helps prevent typos and provides better IDE support.
const (
	// ResourceCPU represents CPU cores requirement
	ResourceCPU = "CPU"
	// ResourceGPU represents GPU devices requirement
	ResourceGPU = "GPU"
	// ResourceMemory represents memory requirement (in bytes)
	ResourceMemory = "memory"
	// ResourceObjectStoreMemory represents object store memory requirement (in bytes)
	ResourceObjectStoreMemory = "object_store_memory"
)

// PlacementGroupPlaceholder is a placeholder for the placement group type.
type PlacementGroupPlaceholder struct{}

// ConcurrencyGroup represents a concurrency group for an actor.
// Concurrency groups allow fine-grained control over actor method concurrency.
// Each group can have its own maximum concurrent calls limit.
//
// This is consistent with Java's io.ray.api.options.ConcurrencyGroup
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

// Validate validates the ConcurrencyGroup configuration.
func (cg *ConcurrencyGroup) Validate() error {
	if cg.Name == "" {
		return fmt.Errorf("concurrency group name cannot be empty")
	}
	if cg.MaxCalls < -1 {
		return fmt.Errorf("invalid MaxCalls: %d, must be >= -1", cg.MaxCalls)
	}
	return nil
}

// ============================================================================
// CallOptions - Task Call Options
// ============================================================================

// CallOptions represents options for task calls.
// Consistent with Java's io.ray.api.options.CallOptions
type CallOptions struct {
	// Name is the task name
	Name string

	// Resources is the resource requirements
	Resources map[string]float64

	// PlacementGroup is the placement group (placeholder)
	PlacementGroup *PlacementGroupPlaceholder

	// PlacementGroupBundleIndex is the bundle index
	PlacementGroupBundleIndex int

	// ConcurrencyGroupName is the concurrency group name
	ConcurrencyGroupName string

	// RuntimeEnv is the runtime environment JSON
	RuntimeEnv string
}

// CallOptionsBuilder is the builder for CallOptions.
// Consistent with Java's CallOptions.Builder
type CallOptionsBuilder struct {
	options CallOptions
}

// NewCallOptionsBuilder creates a new builder.
func NewCallOptionsBuilder() *CallOptionsBuilder {
	return &CallOptionsBuilder{
		options: CallOptions{
			Resources:                 make(map[string]float64),
			PlacementGroupBundleIndex: -1,
		},
	}
}

// WithName sets the task name.
func (b *CallOptionsBuilder) WithName(name string) *CallOptionsBuilder {
	b.options.Name = name
	return b
}

// WithResource sets a single resource requirement.
// Validates that the resource name is non-empty and the value is non-negative.
// Panics if validation fails to catch configuration errors early.
func (b *CallOptionsBuilder) WithResource(name string, value float64) *CallOptionsBuilder {
	if name == "" {
		panic("resource name cannot be empty")
	}
	if value < 0 {
		panic(fmt.Sprintf("resource %s value must be non-negative, got %f", name, value))
	}
	b.options.Resources[name] = value
	return b
}

// WithResources sets multiple resource requirements.
// Validates each resource entry and panics if any validation fails.
func (b *CallOptionsBuilder) WithResources(resources map[string]float64) *CallOptionsBuilder {
	for k, v := range resources {
		if k == "" {
			panic("resource name cannot be empty")
		}
		if v < 0 {
			panic(fmt.Sprintf("resource %s value must be non-negative, got %f", k, v))
		}
		b.options.Resources[k] = v
	}
	return b
}

// WithPlacementGroup sets the placement group.
func (b *CallOptionsBuilder) WithPlacementGroup(group *PlacementGroupPlaceholder, bundleIndex int) *CallOptionsBuilder {
	b.options.PlacementGroup = group
	b.options.PlacementGroupBundleIndex = bundleIndex
	return b
}

// WithConcurrencyGroupName sets the concurrency group name.
func (b *CallOptionsBuilder) WithConcurrencyGroupName(name string) *CallOptionsBuilder {
	b.options.ConcurrencyGroupName = name
	return b
}

// WithRuntimeEnv sets the runtime environment.
func (b *CallOptionsBuilder) WithRuntimeEnv(runtimeEnv string) *CallOptionsBuilder {
	b.options.RuntimeEnv = runtimeEnv
	return b
}

// Build builds the CallOptions.
func (b *CallOptionsBuilder) Build() *CallOptions {
	return &b.options
}

// ConvertToTaskOptions converts CallOptions to submitter.TaskOptions.
func (c *CallOptions) ConvertToTaskOptions() *submitter.TaskOptions {
	return &submitter.TaskOptions{
		Name:       c.Name,
		Resources:  c.Resources,
		RuntimeEnv: c.RuntimeEnv,
	}
}

// Validate validates the CallOptions.
func (c *CallOptions) Validate() error {
	// Validate resource requirements
	for name, quantity := range c.Resources {
		if quantity < 0 {
			return fmt.Errorf("resource %s quantity must be non-negative, got %f", name, quantity)
		}
	}

	// Validate placement group
	if c.PlacementGroup != nil && c.PlacementGroupBundleIndex < -1 {
		return fmt.Errorf("placement group bundle index must be >= -1, got %d", c.PlacementGroupBundleIndex)
	}

	return nil
}

// ============================================================================
// ActorCreationOptions - Actor Creation Options
// ============================================================================

// ActorCreationOptions represents options for actor creation.
// Consistent with Java's io.ray.api.options.ActorCreationOptions
type ActorCreationOptions struct {
	// Name is the actor name
	Name string

	// Namespace is the actor namespace
	Namespace string

	// Resources is the resource requirements
	Resources map[string]float64

	// MaxRestarts is the maximum restart count
	MaxRestarts int

	// MaxTaskRetries is the maximum task retry count
	MaxTaskRetries int

	// MaxConcurrency is the maximum concurrent calls
	MaxConcurrency int

	// MaxPendingCalls is the maximum pending calls (-1 means unlimited)
	MaxPendingCalls int

	PlacementGroup *PlacementGroupPlaceholder

	// PlacementGroupBundleIndex is the bundle index
	PlacementGroupBundleIndex int

	// ConcurrencyGroups is the list of concurrency groups
	ConcurrencyGroups []ConcurrencyGroup

	// RuntimeEnv is the runtime environment JSON
	RuntimeEnv string

	// IsDetached indicates whether this is a detached actor
	IsDetached bool

	// IsAsync indicates whether this is an async actor
	IsAsync bool
}

// ActorCreationOptionsBuilder is the builder for ActorCreationOptions.
// Consistent with Java's ActorCreationOptions.Builder
type ActorCreationOptionsBuilder struct {
	options ActorCreationOptions
}

// NewActorCreationOptionsBuilder creates a new builder.
func NewActorCreationOptionsBuilder() *ActorCreationOptionsBuilder {
	return &ActorCreationOptionsBuilder{
		options: ActorCreationOptions{
			Resources:                 make(map[string]float64),
			MaxRestarts:               0,
			MaxTaskRetries:            0,
			MaxConcurrency:            1,
			MaxPendingCalls:           -1,
			PlacementGroupBundleIndex: -1,
			IsDetached:                false,
			IsAsync:                   false,
		},
	}
}

// WithName sets the actor name and namespace.
func (b *ActorCreationOptionsBuilder) WithName(name string, namespace string) *ActorCreationOptionsBuilder {
	b.options.Name = name
	b.options.Namespace = namespace
	return b
}

// WithResources sets the resource requirements.
// Validates each resource entry and panics if any validation fails.
func (b *ActorCreationOptionsBuilder) WithResources(resources map[string]float64) *ActorCreationOptionsBuilder {
	for name, value := range resources {
		if name == "" {
			panic("resource name cannot be empty")
		}
		if value < 0 {
			panic(fmt.Sprintf("resource %s value must be non-negative, got %f", name, value))
		}
	}
	b.options.Resources = resources
	return b
}

// WithMaxRestarts sets the maximum restart count.
// -1 means infinite restarts.
func (b *ActorCreationOptionsBuilder) WithMaxRestarts(maxRestarts int) *ActorCreationOptionsBuilder {
	b.options.MaxRestarts = maxRestarts
	return b
}

// WithMaxTaskRetries sets the maximum task retry count.
func (b *ActorCreationOptionsBuilder) WithMaxTaskRetries(maxTaskRetries int) *ActorCreationOptionsBuilder {
	b.options.MaxTaskRetries = maxTaskRetries
	return b
}

// WithMaxConcurrency sets the maximum concurrent calls.
func (b *ActorCreationOptionsBuilder) WithMaxConcurrency(maxConcurrency int) *ActorCreationOptionsBuilder {
	if maxConcurrency <= 0 {
		panic("maxConcurrency must be greater than 0")
	}
	b.options.MaxConcurrency = maxConcurrency
	return b
}

// WithMaxPendingCalls sets the maximum pending calls.
// -1 means unlimited.
func (b *ActorCreationOptionsBuilder) WithMaxPendingCalls(maxPendingCalls int) *ActorCreationOptionsBuilder {
	if maxPendingCalls == 0 || maxPendingCalls < -1 {
		panic("maxPendingCalls must be -1 or greater than 0")
	}
	b.options.MaxPendingCalls = maxPendingCalls
	return b
}

// WithPlacementGroup sets the placement group.
func (b *ActorCreationOptionsBuilder) WithPlacementGroup(group *PlacementGroupPlaceholder, bundleIndex int) *ActorCreationOptionsBuilder {
	b.options.PlacementGroup = group
	b.options.PlacementGroupBundleIndex = bundleIndex
	return b
}

// WithConcurrencyGroups sets the concurrency groups.
// Can be called multiple times to append concurrency groups.
func (b *ActorCreationOptionsBuilder) WithConcurrencyGroups(groups ...ConcurrencyGroup) *ActorCreationOptionsBuilder {
	b.options.ConcurrencyGroups = append(b.options.ConcurrencyGroups, groups...)
	return b
}

// WithDefaultConcurrencyGroup sets the default concurrency group.
// The default group contains all methods not explicitly assigned to other groups.
func (b *ActorCreationOptionsBuilder) WithDefaultConcurrencyGroup(maxCalls int) *ActorCreationOptionsBuilder {
	return b.WithConcurrencyGroups(ConcurrencyGroup{
		Name:     "default",
		MaxCalls: maxCalls,
		Methods:  []string{}, // empty list indicates default group
	})
}

// WithRuntimeEnv sets the runtime environment.
func (b *ActorCreationOptionsBuilder) WithRuntimeEnv(runtimeEnv string) *ActorCreationOptionsBuilder {
	b.options.RuntimeEnv = runtimeEnv
	return b
}

// WithDetached sets whether this is a detached actor.
func (b *ActorCreationOptionsBuilder) WithDetached(detached bool) *ActorCreationOptionsBuilder {
	b.options.IsDetached = detached
	return b
}

// WithAsync sets whether this is an async actor.
func (b *ActorCreationOptionsBuilder) WithAsync(async bool) *ActorCreationOptionsBuilder {
	b.options.IsAsync = async
	return b
}

// Build builds the ActorCreationOptions.
func (b *ActorCreationOptionsBuilder) Build() *ActorCreationOptions {
	return &b.options
}

// ConvertToActorCreationOptions converts to submitter.ActorCreationOptions.
func (a *ActorCreationOptions) ConvertToActorCreationOptions() *submitter.ActorCreationOptions {
	// Convert ConcurrencyGroups from api type to submitter type
	var submitterGroups []submitter.ConcurrencyGroup
	if len(a.ConcurrencyGroups) > 0 {
		submitterGroups = make([]submitter.ConcurrencyGroup, len(a.ConcurrencyGroups))
		for i, group := range a.ConcurrencyGroups {
			submitterGroups[i] = submitter.ConcurrencyGroup{
				Name:     group.Name,
				MaxCalls: group.MaxCalls,
				Methods:  group.Methods,
			}
		}
	}

	return &submitter.ActorCreationOptions{
		Name:              a.Name,
		Namespace:         a.Namespace,
		Resources:         a.Resources,
		MaxRestarts:       a.MaxRestarts,
		MaxTaskRetries:    a.MaxTaskRetries,
		RuntimeEnv:        a.RuntimeEnv,
		ConcurrencyGroups: submitterGroups,
	}
}

// Validate validates the ActorCreationOptions.
func (a *ActorCreationOptions) Validate() error {
	// Validate resource requirements
	for name, quantity := range a.Resources {
		if quantity < 0 {
			return fmt.Errorf("resource %s quantity must be non-negative, got %f", name, quantity)
		}
	}

	// Validate maxRestarts
	if a.MaxRestarts < -1 {
		return fmt.Errorf("maxRestarts must be >= -1, got %d", a.MaxRestarts)
	}

	// Validate maxTaskRetries
	if a.MaxTaskRetries < -1 {
		return fmt.Errorf("maxTaskRetries must be >= -1, got %d", a.MaxTaskRetries)
	}

	// Validate maxConcurrency
	if a.MaxConcurrency <= 0 {
		return fmt.Errorf("maxConcurrency must be > 0, got %d", a.MaxConcurrency)
	}

	// Validate maxPendingCalls
	if a.MaxPendingCalls == 0 || a.MaxPendingCalls < -1 {
		return fmt.Errorf("maxPendingCalls must be -1 or > 0, got %d", a.MaxPendingCalls)
	}

	// Validate placement group
	if a.PlacementGroup != nil && a.PlacementGroupBundleIndex < -1 {
		return fmt.Errorf("placement group bundle index must be >= -1, got %d", a.PlacementGroupBundleIndex)
	}

	// Validate concurrency groups
	seenGroups := make(map[string]bool)
	for _, group := range a.ConcurrencyGroups {
		// Validate name uniqueness
		if seenGroups[group.Name] {
			return fmt.Errorf("duplicate concurrency group name: %s", group.Name)
		}
		seenGroups[group.Name] = true

		// Validate individual concurrency group
		if err := group.Validate(); err != nil {
			return err
		}
	}

	// Validate actor name
	if a.Name != "" && a.Namespace == "" {
		// Named actor can have no namespace (using default namespace)
		// Can add name validation rules here
		if len(a.Name) > 255 {
			return fmt.Errorf("actor name must be <= 255 characters, got %d", len(a.Name))
		}
	}

	return nil
}
