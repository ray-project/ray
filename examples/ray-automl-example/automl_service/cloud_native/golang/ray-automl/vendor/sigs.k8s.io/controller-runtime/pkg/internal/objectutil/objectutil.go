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

package objectutil

import (
	"errors"
	"fmt"

	apimeta "k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"sigs.k8s.io/controller-runtime/pkg/client/apiutil"
)

// FilterWithLabels returns a copy of the items in objs matching labelSel.
func FilterWithLabels(objs []runtime.Object, labelSel labels.Selector) ([]runtime.Object, error) {
	outItems := make([]runtime.Object, 0, len(objs))
	for _, obj := range objs {
		meta, err := apimeta.Accessor(obj)
		if err != nil {
			return nil, err
		}
		if labelSel != nil {
			lbls := labels.Set(meta.GetLabels())
			if !labelSel.Matches(lbls) {
				continue
			}
		}
		outItems = append(outItems, obj.DeepCopyObject())
	}
	return outItems, nil
}

// IsAPINamespaced returns true if the object is namespace scoped.
// For unstructured objects the gvk is found from the object itself.
func IsAPINamespaced(obj runtime.Object, scheme *runtime.Scheme, restmapper apimeta.RESTMapper) (bool, error) {
	gvk, err := apiutil.GVKForObject(obj, scheme)
	if err != nil {
		return false, err
	}

	return IsAPINamespacedWithGVK(gvk, scheme, restmapper)
}

// IsAPINamespacedWithGVK returns true if the object having the provided
// GVK is namespace scoped.
func IsAPINamespacedWithGVK(gk schema.GroupVersionKind, scheme *runtime.Scheme, restmapper apimeta.RESTMapper) (bool, error) {
	restmapping, err := restmapper.RESTMapping(schema.GroupKind{Group: gk.Group, Kind: gk.Kind})
	if err != nil {
		return false, fmt.Errorf("failed to get restmapping: %w", err)
	}

	scope := restmapping.Scope.Name()

	if scope == "" {
		return false, errors.New("scope cannot be identified, empty scope returned")
	}

	if scope != apimeta.RESTScopeNameRoot {
		return true, nil
	}
	return false, nil
}
