/*
Copyright 2019 The Kubernetes Authors.

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

package apiutil

import (
	"sync"
	"sync/atomic"

	"golang.org/x/time/rate"
	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/discovery"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/restmapper"
)

// dynamicRESTMapper is a RESTMapper that dynamically discovers resource
// types at runtime.
type dynamicRESTMapper struct {
	mu           sync.RWMutex // protects the following fields
	staticMapper meta.RESTMapper
	limiter      *rate.Limiter
	newMapper    func() (meta.RESTMapper, error)

	lazy bool
	// Used for lazy init.
	inited  uint32
	initMtx sync.Mutex
}

// DynamicRESTMapperOption is a functional option on the dynamicRESTMapper.
type DynamicRESTMapperOption func(*dynamicRESTMapper) error

// WithLimiter sets the RESTMapper's underlying limiter to lim.
func WithLimiter(lim *rate.Limiter) DynamicRESTMapperOption {
	return func(drm *dynamicRESTMapper) error {
		drm.limiter = lim
		return nil
	}
}

// WithLazyDiscovery prevents the RESTMapper from discovering REST mappings
// until an API call is made.
var WithLazyDiscovery DynamicRESTMapperOption = func(drm *dynamicRESTMapper) error {
	drm.lazy = true
	return nil
}

// WithCustomMapper supports setting a custom RESTMapper refresher instead of
// the default method, which uses a discovery client.
//
// This exists mainly for testing, but can be useful if you need tighter control
// over how discovery is performed, which discovery endpoints are queried, etc.
func WithCustomMapper(newMapper func() (meta.RESTMapper, error)) DynamicRESTMapperOption {
	return func(drm *dynamicRESTMapper) error {
		drm.newMapper = newMapper
		return nil
	}
}

// NewDynamicRESTMapper returns a dynamic RESTMapper for cfg. The dynamic
// RESTMapper dynamically discovers resource types at runtime. opts
// configure the RESTMapper.
func NewDynamicRESTMapper(cfg *rest.Config, opts ...DynamicRESTMapperOption) (meta.RESTMapper, error) {
	client, err := discovery.NewDiscoveryClientForConfig(cfg)
	if err != nil {
		return nil, err
	}
	drm := &dynamicRESTMapper{
		limiter: rate.NewLimiter(rate.Limit(defaultRefillRate), defaultLimitSize),
		newMapper: func() (meta.RESTMapper, error) {
			groupResources, err := restmapper.GetAPIGroupResources(client)
			if err != nil {
				return nil, err
			}
			return restmapper.NewDiscoveryRESTMapper(groupResources), nil
		},
	}
	for _, opt := range opts {
		if err = opt(drm); err != nil {
			return nil, err
		}
	}
	if !drm.lazy {
		if err := drm.setStaticMapper(); err != nil {
			return nil, err
		}
	}
	return drm, nil
}

var (
	// defaultRefilRate is the default rate at which potential calls are
	// added back to the "bucket" of allowed calls.
	defaultRefillRate = 5
	// defaultLimitSize is the default starting/max number of potential calls
	// per second.  Once a call is used, it's added back to the bucket at a rate
	// of defaultRefillRate per second.
	defaultLimitSize = 5
)

// setStaticMapper sets drm's staticMapper by querying its client, regardless
// of reload backoff.
func (drm *dynamicRESTMapper) setStaticMapper() error {
	newMapper, err := drm.newMapper()
	if err != nil {
		return err
	}
	drm.staticMapper = newMapper
	return nil
}

// init initializes drm only once if drm is lazy.
func (drm *dynamicRESTMapper) init() (err error) {
	// skip init if drm is not lazy or has initialized
	if !drm.lazy || atomic.LoadUint32(&drm.inited) != 0 {
		return nil
	}

	drm.initMtx.Lock()
	defer drm.initMtx.Unlock()
	if drm.inited == 0 {
		if err = drm.setStaticMapper(); err == nil {
			atomic.StoreUint32(&drm.inited, 1)
		}
	}
	return err
}

// checkAndReload attempts to call the given callback, which is assumed to be dependent
// on the data in the restmapper.
//
// If the callback returns an error matching meta.IsNoMatchErr, it will attempt to reload
// the RESTMapper's data and re-call the callback once that's occurred.
// If the callback returns any other error, the function will return immediately regardless.
//
// It will take care of ensuring that reloads are rate-limited and that extraneous calls
// aren't made. If a reload would exceed the limiters rate, it returns the error return by
// the callback.
// It's thread-safe, and worries about thread-safety for the callback (so the callback does
// not need to attempt to lock the restmapper).
func (drm *dynamicRESTMapper) checkAndReload(checkNeedsReload func() error) error {
	// first, check the common path -- data is fresh enough
	// (use an IIFE for the lock's defer)
	err := func() error {
		drm.mu.RLock()
		defer drm.mu.RUnlock()

		return checkNeedsReload()
	}()

	needsReload := meta.IsNoMatchError(err)
	if !needsReload {
		return err
	}

	// if the data wasn't fresh, we'll need to try and update it, so grab the lock...
	drm.mu.Lock()
	defer drm.mu.Unlock()

	// ... and double-check that we didn't reload in the meantime
	err = checkNeedsReload()
	needsReload = meta.IsNoMatchError(err)
	if !needsReload {
		return err
	}

	// we're still stale, so grab a rate-limit token if we can...
	if !drm.limiter.Allow() {
		// return error from static mapper here, we have refreshed often enough (exceeding rate of provided limiter)
		// so that client's can handle this the same way as a "normal" NoResourceMatchError / NoKindMatchError
		return err
	}

	// ...reload...
	if err := drm.setStaticMapper(); err != nil {
		return err
	}

	// ...and return the results of the closure regardless
	return checkNeedsReload()
}

// TODO: wrap reload errors on NoKindMatchError with go 1.13 errors.

func (drm *dynamicRESTMapper) KindFor(resource schema.GroupVersionResource) (schema.GroupVersionKind, error) {
	if err := drm.init(); err != nil {
		return schema.GroupVersionKind{}, err
	}
	var gvk schema.GroupVersionKind
	err := drm.checkAndReload(func() error {
		var err error
		gvk, err = drm.staticMapper.KindFor(resource)
		return err
	})
	return gvk, err
}

func (drm *dynamicRESTMapper) KindsFor(resource schema.GroupVersionResource) ([]schema.GroupVersionKind, error) {
	if err := drm.init(); err != nil {
		return nil, err
	}
	var gvks []schema.GroupVersionKind
	err := drm.checkAndReload(func() error {
		var err error
		gvks, err = drm.staticMapper.KindsFor(resource)
		return err
	})
	return gvks, err
}

func (drm *dynamicRESTMapper) ResourceFor(input schema.GroupVersionResource) (schema.GroupVersionResource, error) {
	if err := drm.init(); err != nil {
		return schema.GroupVersionResource{}, err
	}

	var gvr schema.GroupVersionResource
	err := drm.checkAndReload(func() error {
		var err error
		gvr, err = drm.staticMapper.ResourceFor(input)
		return err
	})
	return gvr, err
}

func (drm *dynamicRESTMapper) ResourcesFor(input schema.GroupVersionResource) ([]schema.GroupVersionResource, error) {
	if err := drm.init(); err != nil {
		return nil, err
	}
	var gvrs []schema.GroupVersionResource
	err := drm.checkAndReload(func() error {
		var err error
		gvrs, err = drm.staticMapper.ResourcesFor(input)
		return err
	})
	return gvrs, err
}

func (drm *dynamicRESTMapper) RESTMapping(gk schema.GroupKind, versions ...string) (*meta.RESTMapping, error) {
	if err := drm.init(); err != nil {
		return nil, err
	}
	var mapping *meta.RESTMapping
	err := drm.checkAndReload(func() error {
		var err error
		mapping, err = drm.staticMapper.RESTMapping(gk, versions...)
		return err
	})
	return mapping, err
}

func (drm *dynamicRESTMapper) RESTMappings(gk schema.GroupKind, versions ...string) ([]*meta.RESTMapping, error) {
	if err := drm.init(); err != nil {
		return nil, err
	}
	var mappings []*meta.RESTMapping
	err := drm.checkAndReload(func() error {
		var err error
		mappings, err = drm.staticMapper.RESTMappings(gk, versions...)
		return err
	})
	return mappings, err
}

func (drm *dynamicRESTMapper) ResourceSingularizer(resource string) (string, error) {
	if err := drm.init(); err != nil {
		return "", err
	}
	var singular string
	err := drm.checkAndReload(func() error {
		var err error
		singular, err = drm.staticMapper.ResourceSingularizer(resource)
		return err
	})
	return singular, err
}
