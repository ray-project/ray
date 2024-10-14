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

package internal

import (
	"context"
	"time"

	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/cache"
)

// InformersMap create and caches Informers for (runtime.Object, schema.GroupVersionKind) pairs.
// It uses a standard parameter codec constructed based on the given generated Scheme.
type InformersMap struct {
	// we abstract over the details of structured/unstructured/metadata with the specificInformerMaps
	// TODO(directxman12): genericize this over different projections now that we have 3 different maps

	structured   *specificInformersMap
	unstructured *specificInformersMap
	metadata     *specificInformersMap

	// Scheme maps runtime.Objects to GroupVersionKinds
	Scheme *runtime.Scheme
}

// NewInformersMap creates a new InformersMap that can create informers for
// both structured and unstructured objects.
func NewInformersMap(config *rest.Config,
	scheme *runtime.Scheme,
	mapper meta.RESTMapper,
	resync time.Duration,
	namespace string,
	selectors SelectorsByGVK,
	disableDeepCopy DisableDeepCopyByGVK,
	transformers TransformFuncByObject,
) *InformersMap {
	return &InformersMap{
		structured:   newStructuredInformersMap(config, scheme, mapper, resync, namespace, selectors, disableDeepCopy, transformers),
		unstructured: newUnstructuredInformersMap(config, scheme, mapper, resync, namespace, selectors, disableDeepCopy, transformers),
		metadata:     newMetadataInformersMap(config, scheme, mapper, resync, namespace, selectors, disableDeepCopy, transformers),

		Scheme: scheme,
	}
}

// Start calls Run on each of the informers and sets started to true.  Blocks on the context.
func (m *InformersMap) Start(ctx context.Context) error {
	go m.structured.Start(ctx)
	go m.unstructured.Start(ctx)
	go m.metadata.Start(ctx)
	<-ctx.Done()
	return nil
}

// WaitForCacheSync waits until all the caches have been started and synced.
func (m *InformersMap) WaitForCacheSync(ctx context.Context) bool {
	syncedFuncs := append([]cache.InformerSynced(nil), m.structured.HasSyncedFuncs()...)
	syncedFuncs = append(syncedFuncs, m.unstructured.HasSyncedFuncs()...)
	syncedFuncs = append(syncedFuncs, m.metadata.HasSyncedFuncs()...)

	if !m.structured.waitForStarted(ctx) {
		return false
	}
	if !m.unstructured.waitForStarted(ctx) {
		return false
	}
	if !m.metadata.waitForStarted(ctx) {
		return false
	}
	return cache.WaitForCacheSync(ctx.Done(), syncedFuncs...)
}

// Get will create a new Informer and add it to the map of InformersMap if none exists.  Returns
// the Informer from the map.
func (m *InformersMap) Get(ctx context.Context, gvk schema.GroupVersionKind, obj runtime.Object) (bool, *MapEntry, error) {
	switch obj.(type) {
	case *unstructured.Unstructured:
		return m.unstructured.Get(ctx, gvk, obj)
	case *unstructured.UnstructuredList:
		return m.unstructured.Get(ctx, gvk, obj)
	case *metav1.PartialObjectMetadata:
		return m.metadata.Get(ctx, gvk, obj)
	case *metav1.PartialObjectMetadataList:
		return m.metadata.Get(ctx, gvk, obj)
	default:
		return m.structured.Get(ctx, gvk, obj)
	}
}

// newStructuredInformersMap creates a new InformersMap for structured objects.
func newStructuredInformersMap(config *rest.Config, scheme *runtime.Scheme, mapper meta.RESTMapper, resync time.Duration,
	namespace string, selectors SelectorsByGVK, disableDeepCopy DisableDeepCopyByGVK, transformers TransformFuncByObject) *specificInformersMap {
	return newSpecificInformersMap(config, scheme, mapper, resync, namespace, selectors, disableDeepCopy, transformers, createStructuredListWatch)
}

// newUnstructuredInformersMap creates a new InformersMap for unstructured objects.
func newUnstructuredInformersMap(config *rest.Config, scheme *runtime.Scheme, mapper meta.RESTMapper, resync time.Duration,
	namespace string, selectors SelectorsByGVK, disableDeepCopy DisableDeepCopyByGVK, transformers TransformFuncByObject) *specificInformersMap {
	return newSpecificInformersMap(config, scheme, mapper, resync, namespace, selectors, disableDeepCopy, transformers, createUnstructuredListWatch)
}

// newMetadataInformersMap creates a new InformersMap for metadata-only objects.
func newMetadataInformersMap(config *rest.Config, scheme *runtime.Scheme, mapper meta.RESTMapper, resync time.Duration,
	namespace string, selectors SelectorsByGVK, disableDeepCopy DisableDeepCopyByGVK, transformers TransformFuncByObject) *specificInformersMap {
	return newSpecificInformersMap(config, scheme, mapper, resync, namespace, selectors, disableDeepCopy, transformers, createMetadataListWatch)
}
