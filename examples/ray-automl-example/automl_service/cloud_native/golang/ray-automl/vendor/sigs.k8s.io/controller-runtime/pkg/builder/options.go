/*
Copyright 2018 The Kubernetes Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package builder

import (
	"sigs.k8s.io/controller-runtime/pkg/predicate"
)

// {{{ "Functional" Option Interfaces

// ForOption is some configuration that modifies options for a For request.
type ForOption interface {
	// ApplyToFor applies this configuration to the given for input.
	ApplyToFor(*ForInput)
}

// OwnsOption is some configuration that modifies options for a owns request.
type OwnsOption interface {
	// ApplyToOwns applies this configuration to the given owns input.
	ApplyToOwns(*OwnsInput)
}

// WatchesOption is some configuration that modifies options for a watches request.
type WatchesOption interface {
	// ApplyToWatches applies this configuration to the given watches options.
	ApplyToWatches(*WatchesInput)
}

// }}}

// {{{ Multi-Type Options

// WithPredicates sets the given predicates list.
func WithPredicates(predicates ...predicate.Predicate) Predicates {
	return Predicates{
		predicates: predicates,
	}
}

// Predicates filters events before enqueuing the keys.
type Predicates struct {
	predicates []predicate.Predicate
}

// ApplyToFor applies this configuration to the given ForInput options.
func (w Predicates) ApplyToFor(opts *ForInput) {
	opts.predicates = w.predicates
}

// ApplyToOwns applies this configuration to the given OwnsInput options.
func (w Predicates) ApplyToOwns(opts *OwnsInput) {
	opts.predicates = w.predicates
}

// ApplyToWatches applies this configuration to the given WatchesInput options.
func (w Predicates) ApplyToWatches(opts *WatchesInput) {
	opts.predicates = w.predicates
}

var _ ForOption = &Predicates{}
var _ OwnsOption = &Predicates{}
var _ WatchesOption = &Predicates{}

// }}}

// {{{ For & Owns Dual-Type options

// asProjection configures the projection (currently only metadata) on the input.
// Currently only metadata is supported.  We might want to expand
// this to arbitrary non-special local projections in the future.
type projectAs objectProjection

// ApplyToFor applies this configuration to the given ForInput options.
func (p projectAs) ApplyToFor(opts *ForInput) {
	opts.objectProjection = objectProjection(p)
}

// ApplyToOwns applies this configuration to the given OwnsInput options.
func (p projectAs) ApplyToOwns(opts *OwnsInput) {
	opts.objectProjection = objectProjection(p)
}

// ApplyToWatches applies this configuration to the given WatchesInput options.
func (p projectAs) ApplyToWatches(opts *WatchesInput) {
	opts.objectProjection = objectProjection(p)
}

var (
	// OnlyMetadata tells the controller to *only* cache metadata, and to watch
	// the the API server in metadata-only form.  This is useful when watching
	// lots of objects, really big objects, or objects for which you only know
	// the the GVK, but not the structure.  You'll need to pass
	// metav1.PartialObjectMetadata to the client when fetching objects in your
	// reconciler, otherwise you'll end up with a duplicate structured or
	// unstructured cache.
	//
	// When watching a resource with OnlyMetadata, for example the v1.Pod, you
	// should not Get and List using the v1.Pod type. Instead, you should use
	// the special metav1.PartialObjectMetadata type.
	//
	// ❌ Incorrect:
	//
	//   pod := &v1.Pod{}
	//   mgr.GetClient().Get(ctx, nsAndName, pod)
	//
	// ✅ Correct:
	//
	//   pod := &metav1.PartialObjectMetadata{}
	//   pod.SetGroupVersionKind(schema.GroupVersionKind{
	//       Group:   "",
	//       Version: "v1",
	//       Kind:    "Pod",
	//   })
	//   mgr.GetClient().Get(ctx, nsAndName, pod)
	//
	// In the first case, controller-runtime will create another cache for the
	// concrete type on top of the metadata cache; this increases memory
	// consumption and leads to race conditions as caches are not in sync.
	OnlyMetadata = projectAs(projectAsMetadata)

	_ ForOption     = OnlyMetadata
	_ OwnsOption    = OnlyMetadata
	_ WatchesOption = OnlyMetadata
)

// }}}
