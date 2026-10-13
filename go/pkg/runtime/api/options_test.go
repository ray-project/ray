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
	"testing"

	"github.com/ray-project/ray/go/pkg/ids"
	"github.com/ray-project/ray/go/pkg/runtime/submitter"
)

func TestActorCreationOptionsConvertNewFields(t *testing.T) {
	b := NewActorCreationOptionsBuilder().
		WithLifetime(submitter.ActorLifetimeDetached).
		WithAsync(true).
		WithMaxPendingCalls(10)
	opts := b.Build()
	sub := opts.ConvertToActorCreationOptions()
	if sub.Lifetime != submitter.ActorLifetimeDetached {
		t.Errorf("converted Lifetime = %v, want Detached", sub.Lifetime)
	}
	if !sub.IsAsync {
		t.Errorf("converted IsAsync = false, want true")
	}
	if sub.MaxPendingCalls != 10 {
		t.Errorf("converted MaxPendingCalls = %d, want 10", sub.MaxPendingCalls)
	}
}

func TestCallOptionsWithPlacementGroup(t *testing.T) {
	pg := &PlacementGroup{id: ids.OfPlacementGroupID(ids.NewJobID())}
	opts := CallOptions{}
	opts.WithPlacementGroup(pg, 1)
	if opts.PlacementGroup == nil || opts.PlacementGroup.ID() != pg.ID() || opts.PlacementGroupBundleIndex != 1 {
		t.Fatalf("PlacementGroup not set: %+v", opts.PlacementGroup)
	}
}

func TestCallOptionsWithPlacementGroupNil(t *testing.T) {
	opts := CallOptions{PlacementGroup: &PlacementGroup{id: ids.OfPlacementGroupID(ids.NewJobID())}}
	opts.WithPlacementGroup(nil, 1)
	if opts.PlacementGroup == nil {
		t.Fatalf("nil placement group should be a no-op, got nil")
	}
}

func TestCallOptionsConvertPlacementGroup(t *testing.T) {
	pg := &PlacementGroup{id: ids.OfPlacementGroupID(ids.NewJobID())}
	opts := CallOptions{}
	opts.WithPlacementGroup(pg, 1)
	task := opts.ConvertToTaskOptions()
	if task.PlacementGroup == nil {
		t.Fatalf("converted PlacementGroup = nil, want non-nil")
	}
	if task.PlacementGroup.ID != pg.ID() {
		t.Errorf("converted PlacementGroup.ID mismatch")
	}
	if task.PlacementGroup.BundleIndex != 1 {
		t.Errorf("converted PlacementGroup.BundleIndex = %d, want 1", task.PlacementGroup.BundleIndex)
	}
}

func TestActorCreationOptionsWithPlacementGroup(t *testing.T) {
	pg := &PlacementGroup{id: ids.OfPlacementGroupID(ids.NewJobID())}
	opts := ActorCreationOptions{}
	opts.WithPlacementGroup(pg, 2)
	if opts.PlacementGroup == nil || opts.PlacementGroup.ID() != pg.ID() || opts.PlacementGroupBundleIndex != 2 {
		t.Fatalf("PlacementGroup not set: %+v", opts.PlacementGroup)
	}
}

func TestActorCreationOptionsConvertPlacementGroup(t *testing.T) {
	pg := &PlacementGroup{id: ids.OfPlacementGroupID(ids.NewJobID())}
	opts := ActorCreationOptions{}
	opts.WithPlacementGroup(pg, 3)
	sub := opts.ConvertToActorCreationOptions()
	if sub.PlacementGroup == nil {
		t.Fatalf("converted PlacementGroup = nil, want non-nil")
	}
	if sub.PlacementGroup.ID != pg.ID() {
		t.Errorf("converted PlacementGroup.ID mismatch")
	}
	if sub.PlacementGroup.BundleIndex != 3 {
		t.Errorf("converted PlacementGroup.BundleIndex = %d, want 3", sub.PlacementGroup.BundleIndex)
	}
}

func TestCallOptionsBuilderWithPlacementGroupConvert(t *testing.T) {
	pgID := ids.OfPlacementGroupID(ids.NewJobID())
	opts := NewCallOptionsBuilder().WithPlacementGroup(&PlacementGroup{id: pgID}, 4).Build()
	task := opts.ConvertToTaskOptions()
	if task.PlacementGroup == nil {
		t.Fatalf("converted PlacementGroup = nil, want non-nil")
	}
	if task.PlacementGroup.ID != pgID {
		t.Errorf("converted PlacementGroup.ID mismatch")
	}
	if task.PlacementGroup.BundleIndex != 4 {
		t.Errorf("converted PlacementGroup.BundleIndex = %d, want 4", task.PlacementGroup.BundleIndex)
	}
}

func TestActorCreationOptionsBuilderWithPlacementGroupConvert(t *testing.T) {
	pgID := ids.OfPlacementGroupID(ids.NewJobID())
	opts := NewActorCreationOptionsBuilder().WithPlacementGroup(&PlacementGroup{id: pgID}, 5).Build()
	sub := opts.ConvertToActorCreationOptions()
	if sub.PlacementGroup == nil {
		t.Fatalf("converted PlacementGroup = nil, want non-nil")
	}
	if sub.PlacementGroup.ID != pgID {
		t.Errorf("converted PlacementGroup.ID mismatch")
	}
	if sub.PlacementGroup.BundleIndex != 5 {
		t.Errorf("converted PlacementGroup.BundleIndex = %d, want 5", sub.PlacementGroup.BundleIndex)
	}
}

func TestCallOptionsValidatePlacementGroupBundleIndex(t *testing.T) {
	opts := CallOptions{
		PlacementGroup:            &PlacementGroup{id: ids.OfPlacementGroupID(ids.NewJobID())},
		PlacementGroupBundleIndex: -2,
	}
	if err := opts.Validate(); err == nil {
		t.Fatalf("Validate() = nil, want error for invalid BundleIndex -2")
	}
}

func TestActorCreationOptionsValidatePlacementGroupBundleIndex(t *testing.T) {
	opts := ActorCreationOptions{
		MaxConcurrency:            1,
		PlacementGroup:            &PlacementGroup{id: ids.OfPlacementGroupID(ids.NewJobID())},
		PlacementGroupBundleIndex: -2,
	}
	if err := opts.Validate(); err == nil {
		t.Fatalf("Validate() = nil, want error for invalid BundleIndex -2")
	}
}

func TestCallOptionsBuilderWithPlacementGroupNil(t *testing.T) {
	pgID := ids.OfPlacementGroupID(ids.NewJobID())
	b := NewCallOptionsBuilder().WithPlacementGroup(&PlacementGroup{id: pgID}, 4)
	b.WithPlacementGroup(nil, 9)
	opts := b.Build()
	if opts.PlacementGroup == nil {
		t.Fatalf("nil placement group should be a no-op, got nil")
	}
	if opts.PlacementGroupBundleIndex != 4 {
		t.Errorf("legacy PlacementGroupBundleIndex = %d, want 4 unchanged", opts.PlacementGroupBundleIndex)
	}
}

func TestActorCreationOptionsBuilderWithPlacementGroupNil(t *testing.T) {
	pgID := ids.OfPlacementGroupID(ids.NewJobID())
	b := NewActorCreationOptionsBuilder().WithPlacementGroup(&PlacementGroup{id: pgID}, 5)
	b.WithPlacementGroup(nil, 9)
	opts := b.Build()
	if opts.PlacementGroup == nil {
		t.Fatalf("nil placement group should be a no-op, got nil")
	}
	if opts.PlacementGroupBundleIndex != 5 {
		t.Errorf("legacy PlacementGroupBundleIndex = %d, want 5 unchanged", opts.PlacementGroupBundleIndex)
	}
}

func TestCallOptionsWithPlacementGroupSync(t *testing.T) {
	pg := &PlacementGroup{id: ids.OfPlacementGroupID(ids.NewJobID())}
	opts := CallOptions{}
	opts.WithPlacementGroup(pg, 6)
	if opts.PlacementGroupBundleIndex != 6 {
		t.Errorf("legacy PlacementGroupBundleIndex = %d, want 6 (kept in sync)", opts.PlacementGroupBundleIndex)
	}
}
