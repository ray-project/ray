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
	"reflect"
	"strings"

	apimeta "k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/tools/cache"
	"sigs.k8s.io/controller-runtime/pkg/cache/internal"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/apiutil"
)

var (
	_ Informers     = &informerCache{}
	_ client.Reader = &informerCache{}
	_ Cache         = &informerCache{}
)

// ErrCacheNotStarted is returned when trying to read from the cache that wasn't started.
type ErrCacheNotStarted struct{}

func (*ErrCacheNotStarted) Error() string {
	return "the cache is not started, can not read objects"
}

// informerCache is a Kubernetes Object cache populated from InformersMap.  informerCache wraps an InformersMap.
type informerCache struct {
	*internal.InformersMap
}

// Get implements Reader.
func (ip *informerCache) Get(ctx context.Context, key client.ObjectKey, out client.Object, opts ...client.GetOption) error {
	gvk, err := apiutil.GVKForObject(out, ip.Scheme)
	if err != nil {
		return err
	}

	started, cache, err := ip.InformersMap.Get(ctx, gvk, out)
	if err != nil {
		return err
	}

	if !started {
		return &ErrCacheNotStarted{}
	}
	return cache.Reader.Get(ctx, key, out)
}

// List implements Reader.
func (ip *informerCache) List(ctx context.Context, out client.ObjectList, opts ...client.ListOption) error {
	gvk, cacheTypeObj, err := ip.objectTypeForListObject(out)
	if err != nil {
		return err
	}

	started, cache, err := ip.InformersMap.Get(ctx, *gvk, cacheTypeObj)
	if err != nil {
		return err
	}

	if !started {
		return &ErrCacheNotStarted{}
	}

	return cache.Reader.List(ctx, out, opts...)
}

// objectTypeForListObject tries to find the runtime.Object and associated GVK
// for a single object corresponding to the passed-in list type. We need them
// because they are used as cache map key.
func (ip *informerCache) objectTypeForListObject(list client.ObjectList) (*schema.GroupVersionKind, runtime.Object, error) {
	gvk, err := apiutil.GVKForObject(list, ip.Scheme)
	if err != nil {
		return nil, nil, err
	}

	// we need the non-list GVK, so chop off the "List" from the end of the kind
	if strings.HasSuffix(gvk.Kind, "List") && apimeta.IsListType(list) {
		gvk.Kind = gvk.Kind[:len(gvk.Kind)-4]
	}

	_, isUnstructured := list.(*unstructured.UnstructuredList)
	var cacheTypeObj runtime.Object
	if isUnstructured {
		u := &unstructured.Unstructured{}
		u.SetGroupVersionKind(gvk)
		cacheTypeObj = u
	} else {
		itemsPtr, err := apimeta.GetItemsPtr(list)
		if err != nil {
			return nil, nil, err
		}
		// http://knowyourmeme.com/memes/this-is-fine
		elemType := reflect.Indirect(reflect.ValueOf(itemsPtr)).Type().Elem()
		if elemType.Kind() != reflect.Ptr {
			elemType = reflect.PtrTo(elemType)
		}

		cacheTypeValue := reflect.Zero(elemType)
		var ok bool
		cacheTypeObj, ok = cacheTypeValue.Interface().(runtime.Object)
		if !ok {
			return nil, nil, fmt.Errorf("cannot get cache for %T, its element %T is not a runtime.Object", list, cacheTypeValue.Interface())
		}
	}

	return &gvk, cacheTypeObj, nil
}

// GetInformerForKind returns the informer for the GroupVersionKind.
func (ip *informerCache) GetInformerForKind(ctx context.Context, gvk schema.GroupVersionKind) (Informer, error) {
	// Map the gvk to an object
	obj, err := ip.Scheme.New(gvk)
	if err != nil {
		return nil, err
	}

	_, i, err := ip.InformersMap.Get(ctx, gvk, obj)
	if err != nil {
		return nil, err
	}
	return i.Informer, err
}

// GetInformer returns the informer for the obj.
func (ip *informerCache) GetInformer(ctx context.Context, obj client.Object) (Informer, error) {
	gvk, err := apiutil.GVKForObject(obj, ip.Scheme)
	if err != nil {
		return nil, err
	}

	_, i, err := ip.InformersMap.Get(ctx, gvk, obj)
	if err != nil {
		return nil, err
	}
	return i.Informer, err
}

// NeedLeaderElection implements the LeaderElectionRunnable interface
// to indicate that this can be started without requiring the leader lock.
func (ip *informerCache) NeedLeaderElection() bool {
	return false
}

// IndexField adds an indexer to the underlying cache, using extraction function to get
// value(s) from the given field.  This index can then be used by passing a field selector
// to List. For one-to-one compatibility with "normal" field selectors, only return one value.
// The values may be anything.  They will automatically be prefixed with the namespace of the
// given object, if present.  The objects passed are guaranteed to be objects of the correct type.
func (ip *informerCache) IndexField(ctx context.Context, obj client.Object, field string, extractValue client.IndexerFunc) error {
	informer, err := ip.GetInformer(ctx, obj)
	if err != nil {
		return err
	}
	return indexByField(informer, field, extractValue)
}

func indexByField(indexer Informer, field string, extractor client.IndexerFunc) error {
	indexFunc := func(objRaw interface{}) ([]string, error) {
		// TODO(directxman12): check if this is the correct type?
		obj, isObj := objRaw.(client.Object)
		if !isObj {
			return nil, fmt.Errorf("object of type %T is not an Object", objRaw)
		}
		meta, err := apimeta.Accessor(obj)
		if err != nil {
			return nil, err
		}
		ns := meta.GetNamespace()

		rawVals := extractor(obj)
		var vals []string
		if ns == "" {
			// if we're not doubling the keys for the namespaced case, just create a new slice with same length
			vals = make([]string, len(rawVals))
		} else {
			// if we need to add non-namespaced versions too, double the length
			vals = make([]string, len(rawVals)*2)
		}
		for i, rawVal := range rawVals {
			// save a namespaced variant, so that we can ask
			// "what are all the object matching a given index *in a given namespace*"
			vals[i] = internal.KeyToNamespacedKey(ns, rawVal)
			if ns != "" {
				// if we have a namespace, also inject a special index key for listing
				// regardless of the object namespace
				vals[i+len(rawVals)] = internal.KeyToNamespacedKey("", rawVal)
			}
		}

		return vals, nil
	}

	return indexer.AddIndexers(cache.Indexers{internal.FieldIndexName(field): indexFunc})
}
