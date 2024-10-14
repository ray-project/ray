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
	"reflect"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	apimeta "k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/fields"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/selection"
	"k8s.io/client-go/tools/cache"

	"sigs.k8s.io/controller-runtime/pkg/client"
)

// CacheReader is a client.Reader.
var _ client.Reader = &CacheReader{}

// CacheReader wraps a cache.Index to implement the client.CacheReader interface for a single type.
type CacheReader struct {
	// indexer is the underlying indexer wrapped by this cache.
	indexer cache.Indexer

	// groupVersionKind is the group-version-kind of the resource.
	groupVersionKind schema.GroupVersionKind

	// scopeName is the scope of the resource (namespaced or cluster-scoped).
	scopeName apimeta.RESTScopeName

	// disableDeepCopy indicates not to deep copy objects during get or list objects.
	// Be very careful with this, when enabled you must DeepCopy any object before mutating it,
	// otherwise you will mutate the object in the cache.
	disableDeepCopy bool
}

// Get checks the indexer for the object and writes a copy of it if found.
func (c *CacheReader) Get(_ context.Context, key client.ObjectKey, out client.Object, opts ...client.GetOption) error {
	if c.scopeName == apimeta.RESTScopeNameRoot {
		key.Namespace = ""
	}
	storeKey := objectKeyToStoreKey(key)

	// Lookup the object from the indexer cache
	obj, exists, err := c.indexer.GetByKey(storeKey)
	if err != nil {
		return err
	}

	// Not found, return an error
	if !exists {
		// Resource gets transformed into Kind in the error anyway, so this is fine
		return apierrors.NewNotFound(schema.GroupResource{
			Group:    c.groupVersionKind.Group,
			Resource: c.groupVersionKind.Kind,
		}, key.Name)
	}

	// Verify the result is a runtime.Object
	if _, isObj := obj.(runtime.Object); !isObj {
		// This should never happen
		return fmt.Errorf("cache contained %T, which is not an Object", obj)
	}

	if c.disableDeepCopy {
		// skip deep copy which might be unsafe
		// you must DeepCopy any object before mutating it outside
	} else {
		// deep copy to avoid mutating cache
		obj = obj.(runtime.Object).DeepCopyObject()
	}

	// Copy the value of the item in the cache to the returned value
	// TODO(directxman12): this is a terrible hack, pls fix (we should have deepcopyinto)
	outVal := reflect.ValueOf(out)
	objVal := reflect.ValueOf(obj)
	if !objVal.Type().AssignableTo(outVal.Type()) {
		return fmt.Errorf("cache had type %s, but %s was asked for", objVal.Type(), outVal.Type())
	}
	reflect.Indirect(outVal).Set(reflect.Indirect(objVal))
	if !c.disableDeepCopy {
		out.GetObjectKind().SetGroupVersionKind(c.groupVersionKind)
	}

	return nil
}

// List lists items out of the indexer and writes them to out.
func (c *CacheReader) List(_ context.Context, out client.ObjectList, opts ...client.ListOption) error {
	var objs []interface{}
	var err error

	listOpts := client.ListOptions{}
	listOpts.ApplyOptions(opts)

	switch {
	case listOpts.FieldSelector != nil:
		// TODO(directxman12): support more complicated field selectors by
		// combining multiple indices, GetIndexers, etc
		field, val, requiresExact := requiresExactMatch(listOpts.FieldSelector)
		if !requiresExact {
			return fmt.Errorf("non-exact field matches are not supported by the cache")
		}
		// list all objects by the field selector.  If this is namespaced and we have one, ask for the
		// namespaced index key.  Otherwise, ask for the non-namespaced variant by using the fake "all namespaces"
		// namespace.
		objs, err = c.indexer.ByIndex(FieldIndexName(field), KeyToNamespacedKey(listOpts.Namespace, val))
	case listOpts.Namespace != "":
		objs, err = c.indexer.ByIndex(cache.NamespaceIndex, listOpts.Namespace)
	default:
		objs = c.indexer.List()
	}
	if err != nil {
		return err
	}
	var labelSel labels.Selector
	if listOpts.LabelSelector != nil {
		labelSel = listOpts.LabelSelector
	}

	limitSet := listOpts.Limit > 0

	runtimeObjs := make([]runtime.Object, 0, len(objs))
	for _, item := range objs {
		// if the Limit option is set and the number of items
		// listed exceeds this limit, then stop reading.
		if limitSet && int64(len(runtimeObjs)) >= listOpts.Limit {
			break
		}
		obj, isObj := item.(runtime.Object)
		if !isObj {
			return fmt.Errorf("cache contained %T, which is not an Object", obj)
		}
		meta, err := apimeta.Accessor(obj)
		if err != nil {
			return err
		}
		if labelSel != nil {
			lbls := labels.Set(meta.GetLabels())
			if !labelSel.Matches(lbls) {
				continue
			}
		}

		var outObj runtime.Object
		if c.disableDeepCopy {
			// skip deep copy which might be unsafe
			// you must DeepCopy any object before mutating it outside
			outObj = obj
		} else {
			outObj = obj.DeepCopyObject()
			outObj.GetObjectKind().SetGroupVersionKind(c.groupVersionKind)
		}
		runtimeObjs = append(runtimeObjs, outObj)
	}
	return apimeta.SetList(out, runtimeObjs)
}

// objectKeyToStorageKey converts an object key to store key.
// It's akin to MetaNamespaceKeyFunc.  It's separate from
// String to allow keeping the key format easily in sync with
// MetaNamespaceKeyFunc.
func objectKeyToStoreKey(k client.ObjectKey) string {
	if k.Namespace == "" {
		return k.Name
	}
	return k.Namespace + "/" + k.Name
}

// requiresExactMatch checks if the given field selector is of the form `k=v` or `k==v`.
func requiresExactMatch(sel fields.Selector) (field, val string, required bool) {
	reqs := sel.Requirements()
	if len(reqs) != 1 {
		return "", "", false
	}
	req := reqs[0]
	if req.Operator != selection.Equals && req.Operator != selection.DoubleEquals {
		return "", "", false
	}
	return req.Field, req.Value, true
}

// FieldIndexName constructs the name of the index over the given field,
// for use with an indexer.
func FieldIndexName(field string) string {
	return "field:" + field
}

// noNamespaceNamespace is used as the "namespace" when we want to list across all namespaces.
const allNamespacesNamespace = "__all_namespaces"

// KeyToNamespacedKey prefixes the given index key with a namespace
// for use in field selector indexes.
func KeyToNamespacedKey(ns string, baseKey string) string {
	if ns != "" {
		return ns + "/" + baseKey
	}
	return allNamespacesNamespace + "/" + baseKey
}
