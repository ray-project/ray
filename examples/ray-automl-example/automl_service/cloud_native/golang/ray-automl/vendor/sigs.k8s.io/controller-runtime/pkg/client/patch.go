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

package client

import (
	"fmt"

	jsonpatch "github.com/evanphx/json-patch/v5"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/json"
	"k8s.io/apimachinery/pkg/util/strategicpatch"
)

var (
	// Apply uses server-side apply to patch the given object.
	Apply Patch = applyPatch{}

	// Merge uses the raw object as a merge patch, without modifications.
	// Use MergeFrom if you wish to compute a diff instead.
	Merge Patch = mergePatch{}
)

type patch struct {
	patchType types.PatchType
	data      []byte
}

// Type implements Patch.
func (s *patch) Type() types.PatchType {
	return s.patchType
}

// Data implements Patch.
func (s *patch) Data(obj Object) ([]byte, error) {
	return s.data, nil
}

// RawPatch constructs a new Patch with the given PatchType and data.
func RawPatch(patchType types.PatchType, data []byte) Patch {
	return &patch{patchType, data}
}

// MergeFromWithOptimisticLock can be used if clients want to make sure a patch
// is being applied to the latest resource version of an object.
//
// The behavior is similar to what an Update would do, without the need to send the
// whole object. Usually this method is useful if you might have multiple clients
// acting on the same object and the same API version, but with different versions of the Go structs.
//
// For example, an "older" copy of a Widget that has fields A and B, and a "newer" copy with A, B, and C.
// Sending an update using the older struct definition results in C being dropped, whereas using a patch does not.
type MergeFromWithOptimisticLock struct{}

// ApplyToMergeFrom applies this configuration to the given patch options.
func (m MergeFromWithOptimisticLock) ApplyToMergeFrom(in *MergeFromOptions) {
	in.OptimisticLock = true
}

// MergeFromOption is some configuration that modifies options for a merge-from patch data.
type MergeFromOption interface {
	// ApplyToMergeFrom applies this configuration to the given patch options.
	ApplyToMergeFrom(*MergeFromOptions)
}

// MergeFromOptions contains options to generate a merge-from patch data.
type MergeFromOptions struct {
	// OptimisticLock, when true, includes `metadata.resourceVersion` into the final
	// patch data. If the `resourceVersion` field doesn't match what's stored,
	// the operation results in a conflict and clients will need to try again.
	OptimisticLock bool
}

type mergeFromPatch struct {
	patchType   types.PatchType
	createPatch func(originalJSON, modifiedJSON []byte, dataStruct interface{}) ([]byte, error)
	from        Object
	opts        MergeFromOptions
}

// Type implements Patch.
func (s *mergeFromPatch) Type() types.PatchType {
	return s.patchType
}

// Data implements Patch.
func (s *mergeFromPatch) Data(obj Object) ([]byte, error) {
	original := s.from
	modified := obj

	if s.opts.OptimisticLock {
		version := original.GetResourceVersion()
		if len(version) == 0 {
			return nil, fmt.Errorf("cannot use OptimisticLock, object %q does not have any resource version we can use", original)
		}

		original = original.DeepCopyObject().(Object)
		original.SetResourceVersion("")

		modified = modified.DeepCopyObject().(Object)
		modified.SetResourceVersion(version)
	}

	originalJSON, err := json.Marshal(original)
	if err != nil {
		return nil, err
	}

	modifiedJSON, err := json.Marshal(modified)
	if err != nil {
		return nil, err
	}

	data, err := s.createPatch(originalJSON, modifiedJSON, obj)
	if err != nil {
		return nil, err
	}

	return data, nil
}

func createMergePatch(originalJSON, modifiedJSON []byte, _ interface{}) ([]byte, error) {
	return jsonpatch.CreateMergePatch(originalJSON, modifiedJSON)
}

func createStrategicMergePatch(originalJSON, modifiedJSON []byte, dataStruct interface{}) ([]byte, error) {
	return strategicpatch.CreateTwoWayMergePatch(originalJSON, modifiedJSON, dataStruct)
}

// MergeFrom creates a Patch that patches using the merge-patch strategy with the given object as base.
// The difference between MergeFrom and StrategicMergeFrom lays in the handling of modified list fields.
// When using MergeFrom, existing lists will be completely replaced by new lists.
// When using StrategicMergeFrom, the list field's `patchStrategy` is respected if specified in the API type,
// e.g. the existing list is not replaced completely but rather merged with the new one using the list's `patchMergeKey`.
// See https://kubernetes.io/docs/tasks/manage-kubernetes-objects/update-api-object-kubectl-patch/ for more details on
// the difference between merge-patch and strategic-merge-patch.
func MergeFrom(obj Object) Patch {
	return &mergeFromPatch{patchType: types.MergePatchType, createPatch: createMergePatch, from: obj}
}

// MergeFromWithOptions creates a Patch that patches using the merge-patch strategy with the given object as base.
// See MergeFrom for more details.
func MergeFromWithOptions(obj Object, opts ...MergeFromOption) Patch {
	options := &MergeFromOptions{}
	for _, opt := range opts {
		opt.ApplyToMergeFrom(options)
	}
	return &mergeFromPatch{patchType: types.MergePatchType, createPatch: createMergePatch, from: obj, opts: *options}
}

// StrategicMergeFrom creates a Patch that patches using the strategic-merge-patch strategy with the given object as base.
// The difference between MergeFrom and StrategicMergeFrom lays in the handling of modified list fields.
// When using MergeFrom, existing lists will be completely replaced by new lists.
// When using StrategicMergeFrom, the list field's `patchStrategy` is respected if specified in the API type,
// e.g. the existing list is not replaced completely but rather merged with the new one using the list's `patchMergeKey`.
// See https://kubernetes.io/docs/tasks/manage-kubernetes-objects/update-api-object-kubectl-patch/ for more details on
// the difference between merge-patch and strategic-merge-patch.
// Please note, that CRDs don't support strategic-merge-patch, see
// https://kubernetes.io/docs/concepts/extend-kubernetes/api-extension/custom-resources/#advanced-features-and-flexibility
func StrategicMergeFrom(obj Object, opts ...MergeFromOption) Patch {
	options := &MergeFromOptions{}
	for _, opt := range opts {
		opt.ApplyToMergeFrom(options)
	}
	return &mergeFromPatch{patchType: types.StrategicMergePatchType, createPatch: createStrategicMergePatch, from: obj, opts: *options}
}

// mergePatch uses a raw merge strategy to patch the object.
type mergePatch struct{}

// Type implements Patch.
func (p mergePatch) Type() types.PatchType {
	return types.MergePatchType
}

// Data implements Patch.
func (p mergePatch) Data(obj Object) ([]byte, error) {
	// NB(directxman12): we might technically want to be using an actual encoder
	// here (in case some more performant encoder is introduced) but this is
	// correct and sufficient for our uses (it's what the JSON serializer in
	// client-go does, more-or-less).
	return json.Marshal(obj)
}

// applyPatch uses server-side apply to patch the object.
type applyPatch struct{}

// Type implements Patch.
func (p applyPatch) Type() types.PatchType {
	return types.ApplyPatchType
}

// Data implements Patch.
func (p applyPatch) Data(obj Object) ([]byte, error) {
	// NB(directxman12): we might technically want to be using an actual encoder
	// here (in case some more performant encoder is introduced) but this is
	// correct and sufficient for our uses (it's what the JSON serializer in
	// client-go does, more-or-less).
	return json.Marshal(obj)
}
