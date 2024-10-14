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
	"fmt"
	"math/rand"
	"sync"
	"time"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/runtime/serializer"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/metadata"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/cache"

	"sigs.k8s.io/controller-runtime/pkg/client/apiutil"
)

func init() {
	rand.Seed(time.Now().UnixNano())
}

// clientListWatcherFunc knows how to create a ListWatcher.
type createListWatcherFunc func(gvk schema.GroupVersionKind, ip *specificInformersMap) (*cache.ListWatch, error)

// newSpecificInformersMap returns a new specificInformersMap (like
// the generical InformersMap, except that it doesn't implement WaitForCacheSync).
func newSpecificInformersMap(config *rest.Config,
	scheme *runtime.Scheme,
	mapper meta.RESTMapper,
	resync time.Duration,
	namespace string,
	selectors SelectorsByGVK,
	disableDeepCopy DisableDeepCopyByGVK,
	transformers TransformFuncByObject,
	createListWatcher createListWatcherFunc,
) *specificInformersMap {
	ip := &specificInformersMap{
		config:            config,
		Scheme:            scheme,
		mapper:            mapper,
		informersByGVK:    make(map[schema.GroupVersionKind]*MapEntry),
		codecs:            serializer.NewCodecFactory(scheme),
		paramCodec:        runtime.NewParameterCodec(scheme),
		resync:            resync,
		startWait:         make(chan struct{}),
		createListWatcher: createListWatcher,
		namespace:         namespace,
		selectors:         selectors.forGVK,
		disableDeepCopy:   disableDeepCopy,
		transformers:      transformers,
	}
	return ip
}

// MapEntry contains the cached data for an Informer.
type MapEntry struct {
	// Informer is the cached informer
	Informer cache.SharedIndexInformer

	// CacheReader wraps Informer and implements the CacheReader interface for a single type
	Reader CacheReader
}

// specificInformersMap create and caches Informers for (runtime.Object, schema.GroupVersionKind) pairs.
// It uses a standard parameter codec constructed based on the given generated Scheme.
type specificInformersMap struct {
	// Scheme maps runtime.Objects to GroupVersionKinds
	Scheme *runtime.Scheme

	// config is used to talk to the apiserver
	config *rest.Config

	// mapper maps GroupVersionKinds to Resources
	mapper meta.RESTMapper

	// informersByGVK is the cache of informers keyed by groupVersionKind
	informersByGVK map[schema.GroupVersionKind]*MapEntry

	// codecs is used to create a new REST client
	codecs serializer.CodecFactory

	// paramCodec is used by list and watch
	paramCodec runtime.ParameterCodec

	// stop is the stop channel to stop informers
	stop <-chan struct{}

	// resync is the base frequency the informers are resynced
	// a 10 percent jitter will be added to the resync period between informers
	// so that all informers will not send list requests simultaneously.
	resync time.Duration

	// mu guards access to the map
	mu sync.RWMutex

	// start is true if the informers have been started
	started bool

	// startWait is a channel that is closed after the
	// informer has been started.
	startWait chan struct{}

	// createClient knows how to create a client and a list object,
	// and allows for abstracting over the particulars of structured vs
	// unstructured objects.
	createListWatcher createListWatcherFunc

	// namespace is the namespace that all ListWatches are restricted to
	// default or empty string means all namespaces
	namespace string

	// selectors are the label or field selectors that will be added to the
	// ListWatch ListOptions.
	selectors func(gvk schema.GroupVersionKind) Selector

	// disableDeepCopy indicates not to deep copy objects during get or list objects.
	disableDeepCopy DisableDeepCopyByGVK

	// transform funcs are applied to objects before they are committed to the cache
	transformers TransformFuncByObject
}

// Start calls Run on each of the informers and sets started to true.  Blocks on the context.
// It doesn't return start because it can't return an error, and it's not a runnable directly.
func (ip *specificInformersMap) Start(ctx context.Context) {
	func() {
		ip.mu.Lock()
		defer ip.mu.Unlock()

		// Set the stop channel so it can be passed to informers that are added later
		ip.stop = ctx.Done()

		// Start each informer
		for _, informer := range ip.informersByGVK {
			go informer.Informer.Run(ctx.Done())
		}

		// Set started to true so we immediately start any informers added later.
		ip.started = true
		close(ip.startWait)
	}()
	<-ctx.Done()
}

func (ip *specificInformersMap) waitForStarted(ctx context.Context) bool {
	select {
	case <-ip.startWait:
		return true
	case <-ctx.Done():
		return false
	}
}

// HasSyncedFuncs returns all the HasSynced functions for the informers in this map.
func (ip *specificInformersMap) HasSyncedFuncs() []cache.InformerSynced {
	ip.mu.RLock()
	defer ip.mu.RUnlock()
	syncedFuncs := make([]cache.InformerSynced, 0, len(ip.informersByGVK))
	for _, informer := range ip.informersByGVK {
		syncedFuncs = append(syncedFuncs, informer.Informer.HasSynced)
	}
	return syncedFuncs
}

// Get will create a new Informer and add it to the map of specificInformersMap if none exists.  Returns
// the Informer from the map.
func (ip *specificInformersMap) Get(ctx context.Context, gvk schema.GroupVersionKind, obj runtime.Object) (bool, *MapEntry, error) {
	// Return the informer if it is found
	i, started, ok := func() (*MapEntry, bool, bool) {
		ip.mu.RLock()
		defer ip.mu.RUnlock()
		i, ok := ip.informersByGVK[gvk]
		return i, ip.started, ok
	}()

	if !ok {
		var err error
		if i, started, err = ip.addInformerToMap(gvk, obj); err != nil {
			return started, nil, err
		}
	}

	if started && !i.Informer.HasSynced() {
		// Wait for it to sync before returning the Informer so that folks don't read from a stale cache.
		if !cache.WaitForCacheSync(ctx.Done(), i.Informer.HasSynced) {
			return started, nil, apierrors.NewTimeoutError(fmt.Sprintf("failed waiting for %T Informer to sync", obj), 0)
		}
	}

	return started, i, nil
}

func (ip *specificInformersMap) addInformerToMap(gvk schema.GroupVersionKind, obj runtime.Object) (*MapEntry, bool, error) {
	ip.mu.Lock()
	defer ip.mu.Unlock()

	// Check the cache to see if we already have an Informer.  If we do, return the Informer.
	// This is for the case where 2 routines tried to get the informer when it wasn't in the map
	// so neither returned early, but the first one created it.
	if i, ok := ip.informersByGVK[gvk]; ok {
		return i, ip.started, nil
	}

	// Create a NewSharedIndexInformer and add it to the map.
	var lw *cache.ListWatch
	lw, err := ip.createListWatcher(gvk, ip)
	if err != nil {
		return nil, false, err
	}
	ni := cache.NewSharedIndexInformer(lw, obj, resyncPeriod(ip.resync)(), cache.Indexers{
		cache.NamespaceIndex: cache.MetaNamespaceIndexFunc,
	})

	// Check to see if there is a transformer for this gvk
	if err := ni.SetTransform(ip.transformers.Get(gvk)); err != nil {
		return nil, false, err
	}

	rm, err := ip.mapper.RESTMapping(gvk.GroupKind(), gvk.Version)
	if err != nil {
		return nil, false, err
	}

	i := &MapEntry{
		Informer: ni,
		Reader: CacheReader{
			indexer:          ni.GetIndexer(),
			groupVersionKind: gvk,
			scopeName:        rm.Scope.Name(),
			disableDeepCopy:  ip.disableDeepCopy.IsDisabled(gvk),
		},
	}
	ip.informersByGVK[gvk] = i

	// Start the Informer if need by
	// TODO(seans): write thorough tests and document what happens here - can you add indexers?
	// can you add eventhandlers?
	if ip.started {
		go i.Informer.Run(ip.stop)
	}
	return i, ip.started, nil
}

// newListWatch returns a new ListWatch object that can be used to create a SharedIndexInformer.
func createStructuredListWatch(gvk schema.GroupVersionKind, ip *specificInformersMap) (*cache.ListWatch, error) {
	// Kubernetes APIs work against Resources, not GroupVersionKinds.  Map the
	// groupVersionKind to the Resource API we will use.
	mapping, err := ip.mapper.RESTMapping(gvk.GroupKind(), gvk.Version)
	if err != nil {
		return nil, err
	}

	client, err := apiutil.RESTClientForGVK(gvk, false, ip.config, ip.codecs)
	if err != nil {
		return nil, err
	}
	listGVK := gvk.GroupVersion().WithKind(gvk.Kind + "List")
	listObj, err := ip.Scheme.New(listGVK)
	if err != nil {
		return nil, err
	}

	// TODO: the functions that make use of this ListWatch should be adapted to
	//  pass in their own contexts instead of relying on this fixed one here.
	ctx := context.TODO()
	// Create a new ListWatch for the obj
	return &cache.ListWatch{
		ListFunc: func(opts metav1.ListOptions) (runtime.Object, error) {
			ip.selectors(gvk).ApplyToList(&opts)
			res := listObj.DeepCopyObject()
			namespace := restrictNamespaceBySelector(ip.namespace, ip.selectors(gvk))
			isNamespaceScoped := namespace != "" && mapping.Scope.Name() != meta.RESTScopeNameRoot
			err := client.Get().NamespaceIfScoped(namespace, isNamespaceScoped).Resource(mapping.Resource.Resource).VersionedParams(&opts, ip.paramCodec).Do(ctx).Into(res)
			return res, err
		},
		// Setup the watch function
		WatchFunc: func(opts metav1.ListOptions) (watch.Interface, error) {
			ip.selectors(gvk).ApplyToList(&opts)
			// Watch needs to be set to true separately
			opts.Watch = true
			namespace := restrictNamespaceBySelector(ip.namespace, ip.selectors(gvk))
			isNamespaceScoped := namespace != "" && mapping.Scope.Name() != meta.RESTScopeNameRoot
			return client.Get().NamespaceIfScoped(namespace, isNamespaceScoped).Resource(mapping.Resource.Resource).VersionedParams(&opts, ip.paramCodec).Watch(ctx)
		},
	}, nil
}

func createUnstructuredListWatch(gvk schema.GroupVersionKind, ip *specificInformersMap) (*cache.ListWatch, error) {
	// Kubernetes APIs work against Resources, not GroupVersionKinds.  Map the
	// groupVersionKind to the Resource API we will use.
	mapping, err := ip.mapper.RESTMapping(gvk.GroupKind(), gvk.Version)
	if err != nil {
		return nil, err
	}

	// If the rest configuration has a negotiated serializer passed in,
	// we should remove it and use the one that the dynamic client sets for us.
	cfg := rest.CopyConfig(ip.config)
	cfg.NegotiatedSerializer = nil
	dynamicClient, err := dynamic.NewForConfig(cfg)
	if err != nil {
		return nil, err
	}

	// TODO: the functions that make use of this ListWatch should be adapted to
	//  pass in their own contexts instead of relying on this fixed one here.
	ctx := context.TODO()
	// Create a new ListWatch for the obj
	return &cache.ListWatch{
		ListFunc: func(opts metav1.ListOptions) (runtime.Object, error) {
			ip.selectors(gvk).ApplyToList(&opts)
			namespace := restrictNamespaceBySelector(ip.namespace, ip.selectors(gvk))
			if namespace != "" && mapping.Scope.Name() != meta.RESTScopeNameRoot {
				return dynamicClient.Resource(mapping.Resource).Namespace(namespace).List(ctx, opts)
			}
			return dynamicClient.Resource(mapping.Resource).List(ctx, opts)
		},
		// Setup the watch function
		WatchFunc: func(opts metav1.ListOptions) (watch.Interface, error) {
			ip.selectors(gvk).ApplyToList(&opts)
			// Watch needs to be set to true separately
			opts.Watch = true
			namespace := restrictNamespaceBySelector(ip.namespace, ip.selectors(gvk))
			if namespace != "" && mapping.Scope.Name() != meta.RESTScopeNameRoot {
				return dynamicClient.Resource(mapping.Resource).Namespace(namespace).Watch(ctx, opts)
			}
			return dynamicClient.Resource(mapping.Resource).Watch(ctx, opts)
		},
	}, nil
}

func createMetadataListWatch(gvk schema.GroupVersionKind, ip *specificInformersMap) (*cache.ListWatch, error) {
	// Kubernetes APIs work against Resources, not GroupVersionKinds.  Map the
	// groupVersionKind to the Resource API we will use.
	mapping, err := ip.mapper.RESTMapping(gvk.GroupKind(), gvk.Version)
	if err != nil {
		return nil, err
	}

	// Always clear the negotiated serializer and use the one
	// set from the metadata client.
	cfg := rest.CopyConfig(ip.config)
	cfg.NegotiatedSerializer = nil

	// grab the metadata client
	client, err := metadata.NewForConfig(cfg)
	if err != nil {
		return nil, err
	}

	// TODO: the functions that make use of this ListWatch should be adapted to
	//  pass in their own contexts instead of relying on this fixed one here.
	ctx := context.TODO()

	// create the relevant listwatch
	return &cache.ListWatch{
		ListFunc: func(opts metav1.ListOptions) (runtime.Object, error) {
			ip.selectors(gvk).ApplyToList(&opts)

			var (
				list *metav1.PartialObjectMetadataList
				err  error
			)
			namespace := restrictNamespaceBySelector(ip.namespace, ip.selectors(gvk))
			if namespace != "" && mapping.Scope.Name() != meta.RESTScopeNameRoot {
				list, err = client.Resource(mapping.Resource).Namespace(namespace).List(ctx, opts)
			} else {
				list, err = client.Resource(mapping.Resource).List(ctx, opts)
			}
			if list != nil {
				for i := range list.Items {
					list.Items[i].SetGroupVersionKind(gvk)
				}
			}
			return list, err
		},
		// Setup the watch function
		WatchFunc: func(opts metav1.ListOptions) (watch.Interface, error) {
			ip.selectors(gvk).ApplyToList(&opts)
			// Watch needs to be set to true separately
			opts.Watch = true

			var (
				watcher watch.Interface
				err     error
			)
			namespace := restrictNamespaceBySelector(ip.namespace, ip.selectors(gvk))
			if namespace != "" && mapping.Scope.Name() != meta.RESTScopeNameRoot {
				watcher, err = client.Resource(mapping.Resource).Namespace(namespace).Watch(ctx, opts)
			} else {
				watcher, err = client.Resource(mapping.Resource).Watch(ctx, opts)
			}
			if watcher != nil {
				watcher = newGVKFixupWatcher(gvk, watcher)
			}
			return watcher, err
		},
	}, nil
}

// newGVKFixupWatcher adds a wrapper that preserves the GVK information when
// events come in.
//
// This works around a bug where GVK information is not passed into mapping
// functions when using the OnlyMetadata option in the builder.
// This issue is most likely caused by kubernetes/kubernetes#80609.
// See kubernetes-sigs/controller-runtime#1484.
//
// This was originally implemented as a cache.ResourceEventHandler wrapper but
// that contained a data race which was resolved by setting the GVK in a watch
// wrapper, before the objects are written to the cache.
// See kubernetes-sigs/controller-runtime#1650.
//
// The original watch wrapper was found to be incompatible with
// k8s.io/client-go/tools/cache.Reflector so it has been re-implemented as a
// watch.Filter which is compatible.
// See kubernetes-sigs/controller-runtime#1789.
func newGVKFixupWatcher(gvk schema.GroupVersionKind, watcher watch.Interface) watch.Interface {
	return watch.Filter(
		watcher,
		func(in watch.Event) (watch.Event, bool) {
			in.Object.GetObjectKind().SetGroupVersionKind(gvk)
			return in, true
		},
	)
}

// resyncPeriod returns a function which generates a duration each time it is
// invoked; this is so that multiple controllers don't get into lock-step and all
// hammer the apiserver with list requests simultaneously.
func resyncPeriod(resync time.Duration) func() time.Duration {
	return func() time.Duration {
		// the factor will fall into [0.9, 1.1)
		factor := rand.Float64()/5.0 + 0.9 //nolint:gosec
		return time.Duration(float64(resync.Nanoseconds()) * factor)
	}
}

// restrictNamespaceBySelector returns either a global restriction for all ListWatches
// if not default/empty, or the namespace that a ListWatch for the specific resource
// is restricted to, based on a specified field selector for metadata.namespace field.
func restrictNamespaceBySelector(namespaceOpt string, s Selector) string {
	if namespaceOpt != "" {
		// namespace is already restricted
		return namespaceOpt
	}
	fieldSelector := s.Field
	if fieldSelector == nil || fieldSelector.Empty() {
		return ""
	}
	// check whether a selector includes the namespace field
	value, found := fieldSelector.RequiresExactMatch("metadata.namespace")
	if found {
		return value
	}
	return ""
}
