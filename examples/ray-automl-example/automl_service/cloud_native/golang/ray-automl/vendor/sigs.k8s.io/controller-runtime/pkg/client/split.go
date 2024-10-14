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
	"context"
	"strings"

	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"

	"sigs.k8s.io/controller-runtime/pkg/client/apiutil"
)

// NewDelegatingClientInput encapsulates the input parameters to create a new delegating client.
type NewDelegatingClientInput struct {
	CacheReader       Reader
	Client            Client
	UncachedObjects   []Object
	CacheUnstructured bool
}

// NewDelegatingClient creates a new delegating client.
//
// A delegating client forms a Client by composing separate reader, writer and
// statusclient interfaces.  This way, you can have an Client that reads from a
// cache and writes to the API server.
func NewDelegatingClient(in NewDelegatingClientInput) (Client, error) {
	uncachedGVKs := map[schema.GroupVersionKind]struct{}{}
	for _, obj := range in.UncachedObjects {
		gvk, err := apiutil.GVKForObject(obj, in.Client.Scheme())
		if err != nil {
			return nil, err
		}
		uncachedGVKs[gvk] = struct{}{}
	}

	return &delegatingClient{
		scheme: in.Client.Scheme(),
		mapper: in.Client.RESTMapper(),
		Reader: &delegatingReader{
			CacheReader:       in.CacheReader,
			ClientReader:      in.Client,
			scheme:            in.Client.Scheme(),
			uncachedGVKs:      uncachedGVKs,
			cacheUnstructured: in.CacheUnstructured,
		},
		Writer:       in.Client,
		StatusClient: in.Client,
	}, nil
}

type delegatingClient struct {
	Reader
	Writer
	StatusClient

	scheme *runtime.Scheme
	mapper meta.RESTMapper
}

// Scheme returns the scheme this client is using.
func (d *delegatingClient) Scheme() *runtime.Scheme {
	return d.scheme
}

// RESTMapper returns the rest mapper this client is using.
func (d *delegatingClient) RESTMapper() meta.RESTMapper {
	return d.mapper
}

// delegatingReader forms a Reader that will cause Get and List requests for
// unstructured types to use the ClientReader while requests for any other type
// of object with use the CacheReader.  This avoids accidentally caching the
// entire cluster in the common case of loading arbitrary unstructured objects
// (e.g. from OwnerReferences).
type delegatingReader struct {
	CacheReader  Reader
	ClientReader Reader

	uncachedGVKs      map[schema.GroupVersionKind]struct{}
	scheme            *runtime.Scheme
	cacheUnstructured bool
}

func (d *delegatingReader) shouldBypassCache(obj runtime.Object) (bool, error) {
	gvk, err := apiutil.GVKForObject(obj, d.scheme)
	if err != nil {
		return false, err
	}
	// TODO: this is producing unsafe guesses that don't actually work,
	// but it matches ~99% of the cases out there.
	if meta.IsListType(obj) {
		gvk.Kind = strings.TrimSuffix(gvk.Kind, "List")
	}
	if _, isUncached := d.uncachedGVKs[gvk]; isUncached {
		return true, nil
	}
	if !d.cacheUnstructured {
		_, isUnstructured := obj.(*unstructured.Unstructured)
		_, isUnstructuredList := obj.(*unstructured.UnstructuredList)
		return isUnstructured || isUnstructuredList, nil
	}
	return false, nil
}

// Get retrieves an obj for a given object key from the Kubernetes Cluster.
func (d *delegatingReader) Get(ctx context.Context, key ObjectKey, obj Object, opts ...GetOption) error {
	if isUncached, err := d.shouldBypassCache(obj); err != nil {
		return err
	} else if isUncached {
		return d.ClientReader.Get(ctx, key, obj, opts...)
	}
	return d.CacheReader.Get(ctx, key, obj, opts...)
}

// List retrieves list of objects for a given namespace and list options.
func (d *delegatingReader) List(ctx context.Context, list ObjectList, opts ...ListOption) error {
	if isUncached, err := d.shouldBypassCache(list); err != nil {
		return err
	} else if isUncached {
		return d.ClientReader.List(ctx, list, opts...)
	}
	return d.CacheReader.List(ctx, list, opts...)
}
