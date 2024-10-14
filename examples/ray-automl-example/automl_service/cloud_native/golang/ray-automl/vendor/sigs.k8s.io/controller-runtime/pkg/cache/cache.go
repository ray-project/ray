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

package cache

import (
	"context"
	"fmt"
	"time"

	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/rest"
	toolscache "k8s.io/client-go/tools/cache"
	"sigs.k8s.io/controller-runtime/pkg/cache/internal"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/apiutil"
	logf "sigs.k8s.io/controller-runtime/pkg/internal/log"
)

var log = logf.RuntimeLog.WithName("object-cache")

// Cache knows how to load Kubernetes objects, fetch informers to request
// to receive events for Kubernetes objects (at a low-level),
// and add indices to fields on the objects stored in the cache.
type Cache interface {
	// Cache acts as a client to objects stored in the cache.
	client.Reader

	// Cache loads informers and adds field indices.
	Informers
}

// Informers knows how to create or fetch informers for different
// group-version-kinds, and add indices to those informers.  It's safe to call
// GetInformer from multiple threads.
type Informers interface {
	// GetInformer fetches or constructs an informer for the given object that corresponds to a single
	// API kind and resource.
	GetInformer(ctx context.Context, obj client.Object) (Informer, error)

	// GetInformerForKind is similar to GetInformer, except that it takes a group-version-kind, instead
	// of the underlying object.
	GetInformerForKind(ctx context.Context, gvk schema.GroupVersionKind) (Informer, error)

	// Start runs all the informers known to this cache until the context is closed.
	// It blocks.
	Start(ctx context.Context) error

	// WaitForCacheSync waits for all the caches to sync.  Returns false if it could not sync a cache.
	WaitForCacheSync(ctx context.Context) bool

	// Informers knows how to add indices to the caches (informers) that it manages.
	client.FieldIndexer
}

// Informer - informer allows you interact with the underlying informer.
type Informer interface {
	// AddEventHandler adds an event handler to the shared informer using the shared informer's resync
	// period.  Events to a single handler are delivered sequentially, but there is no coordination
	// between different handlers.
	AddEventHandler(handler toolscache.ResourceEventHandler)
	// AddEventHandlerWithResyncPeriod adds an event handler to the shared informer using the
	// specified resync period.  Events to a single handler are delivered sequentially, but there is
	// no coordination between different handlers.
	AddEventHandlerWithResyncPeriod(handler toolscache.ResourceEventHandler, resyncPeriod time.Duration)
	// AddIndexers adds more indexers to this store.  If you call this after you already have data
	// in the store, the results are undefined.
	AddIndexers(indexers toolscache.Indexers) error
	// HasSynced return true if the informers underlying store has synced.
	HasSynced() bool
}

// ObjectSelector is an alias name of internal.Selector.
type ObjectSelector internal.Selector

// SelectorsByObject associate a client.Object's GVK to a field/label selector.
// There is also `DefaultSelector` to set a global default (which will be overridden by
// a more specific setting here, if any).
type SelectorsByObject map[client.Object]ObjectSelector

// Options are the optional arguments for creating a new InformersMap object.
type Options struct {
	// Scheme is the scheme to use for mapping objects to GroupVersionKinds
	Scheme *runtime.Scheme

	// Mapper is the RESTMapper to use for mapping GroupVersionKinds to Resources
	Mapper meta.RESTMapper

	// Resync is the base frequency the informers are resynced.
	// Defaults to defaultResyncTime.
	// A 10 percent jitter will be added to the Resync period between informers
	// So that all informers will not send list requests simultaneously.
	Resync *time.Duration

	// Namespace restricts the cache's ListWatch to the desired namespace
	// Default watches all namespaces
	Namespace string

	// SelectorsByObject restricts the cache's ListWatch to the desired
	// fields per GVK at the specified object, the map's value must implement
	// Selector [1] using for example a Set [2]
	// [1] https://pkg.go.dev/k8s.io/apimachinery/pkg/fields#Selector
	// [2] https://pkg.go.dev/k8s.io/apimachinery/pkg/fields#Set
	SelectorsByObject SelectorsByObject

	// DefaultSelector will be used as selectors for all object types
	// that do not have a selector in SelectorsByObject defined.
	DefaultSelector ObjectSelector

	// UnsafeDisableDeepCopyByObject indicates not to deep copy objects during get or
	// list objects per GVK at the specified object.
	// Be very careful with this, when enabled you must DeepCopy any object before mutating it,
	// otherwise you will mutate the object in the cache.
	UnsafeDisableDeepCopyByObject DisableDeepCopyByObject

	// TransformByObject is a map from GVKs to transformer functions which
	// get applied when objects of the transformation are about to be committed
	// to cache.
	//
	// This function is called both for new objects to enter the cache,
	// 	and for updated objects.
	TransformByObject TransformByObject

	// DefaultTransform is the transform used for all GVKs which do
	// not have an explicit transform func set in TransformByObject
	DefaultTransform toolscache.TransformFunc
}

var defaultResyncTime = 10 * time.Hour

// New initializes and returns a new Cache.
func New(config *rest.Config, opts Options) (Cache, error) {
	opts, err := defaultOpts(config, opts)
	if err != nil {
		return nil, err
	}
	selectorsByGVK, err := convertToSelectorsByGVK(opts.SelectorsByObject, opts.DefaultSelector, opts.Scheme)
	if err != nil {
		return nil, err
	}
	disableDeepCopyByGVK, err := convertToDisableDeepCopyByGVK(opts.UnsafeDisableDeepCopyByObject, opts.Scheme)
	if err != nil {
		return nil, err
	}
	transformByGVK, err := convertToTransformByKindAndGVK(opts.TransformByObject, opts.DefaultTransform, opts.Scheme)
	if err != nil {
		return nil, err
	}

	im := internal.NewInformersMap(config, opts.Scheme, opts.Mapper, *opts.Resync, opts.Namespace, selectorsByGVK, disableDeepCopyByGVK, transformByGVK)
	return &informerCache{InformersMap: im}, nil
}

// BuilderWithOptions returns a Cache constructor that will build the a cache
// honoring the options argument, this is useful to specify options like
// SelectorsByObject
// WARNING: If SelectorsByObject is specified, filtered out resources are not
// returned.
// WARNING: If UnsafeDisableDeepCopy is enabled, you must DeepCopy any object
// returned from cache get/list before mutating it.
func BuilderWithOptions(options Options) NewCacheFunc {
	return func(config *rest.Config, opts Options) (Cache, error) {
		if options.Scheme == nil {
			options.Scheme = opts.Scheme
		}
		if options.Mapper == nil {
			options.Mapper = opts.Mapper
		}
		if options.Resync == nil {
			options.Resync = opts.Resync
		}
		if options.Namespace == "" {
			options.Namespace = opts.Namespace
		}
		if opts.Resync == nil {
			opts.Resync = options.Resync
		}

		return New(config, options)
	}
}

func defaultOpts(config *rest.Config, opts Options) (Options, error) {
	// Use the default Kubernetes Scheme if unset
	if opts.Scheme == nil {
		opts.Scheme = scheme.Scheme
	}

	// Construct a new Mapper if unset
	if opts.Mapper == nil {
		var err error
		opts.Mapper, err = apiutil.NewDiscoveryRESTMapper(config)
		if err != nil {
			log.WithName("setup").Error(err, "Failed to get API Group-Resources")
			return opts, fmt.Errorf("could not create RESTMapper from config")
		}
	}

	// Default the resync period to 10 hours if unset
	if opts.Resync == nil {
		opts.Resync = &defaultResyncTime
	}
	return opts, nil
}

func convertToSelectorsByGVK(selectorsByObject SelectorsByObject, defaultSelector ObjectSelector, scheme *runtime.Scheme) (internal.SelectorsByGVK, error) {
	selectorsByGVK := internal.SelectorsByGVK{}
	for object, selector := range selectorsByObject {
		gvk, err := apiutil.GVKForObject(object, scheme)
		if err != nil {
			return nil, err
		}
		selectorsByGVK[gvk] = internal.Selector(selector)
	}
	selectorsByGVK[schema.GroupVersionKind{}] = internal.Selector(defaultSelector)
	return selectorsByGVK, nil
}

// DisableDeepCopyByObject associate a client.Object's GVK to disable DeepCopy during get or list from cache.
type DisableDeepCopyByObject map[client.Object]bool

var _ client.Object = &ObjectAll{}

// ObjectAll is the argument to represent all objects' types.
type ObjectAll struct {
	client.Object
}

func convertToDisableDeepCopyByGVK(disableDeepCopyByObject DisableDeepCopyByObject, scheme *runtime.Scheme) (internal.DisableDeepCopyByGVK, error) {
	disableDeepCopyByGVK := internal.DisableDeepCopyByGVK{}
	for obj, disable := range disableDeepCopyByObject {
		switch obj.(type) {
		case ObjectAll, *ObjectAll:
			disableDeepCopyByGVK[internal.GroupVersionKindAll] = disable
		default:
			gvk, err := apiutil.GVKForObject(obj, scheme)
			if err != nil {
				return nil, err
			}
			disableDeepCopyByGVK[gvk] = disable
		}
	}
	return disableDeepCopyByGVK, nil
}

// TransformByObject associate a client.Object's GVK to a transformer function
// to be applied when storing the object into the cache.
type TransformByObject map[client.Object]toolscache.TransformFunc

func convertToTransformByKindAndGVK(t TransformByObject, defaultTransform toolscache.TransformFunc, scheme *runtime.Scheme) (internal.TransformFuncByObject, error) {
	result := internal.NewTransformFuncByObject()
	for obj, transformation := range t {
		if err := result.Set(obj, scheme, transformation); err != nil {
			return nil, err
		}
	}
	result.SetDefault(defaultTransform)
	return result, nil
}
