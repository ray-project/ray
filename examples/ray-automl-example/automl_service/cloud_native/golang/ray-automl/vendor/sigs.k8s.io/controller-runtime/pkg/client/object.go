/*
Copyright 2020 The Kubernetes Authors.

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
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
)

// Object is a Kubernetes object, allows functions to work indistinctly with
// any resource that implements both Object interfaces.
//
// Semantically, these are objects which are both serializable (runtime.Object)
// and identifiable (metav1.Object) -- think any object which you could write
// as YAML or JSON, and then `kubectl create`.
//
// Code-wise, this means that any object which embeds both ObjectMeta (which
// provides metav1.Object) and TypeMeta (which provides half of runtime.Object)
// and has a `DeepCopyObject` implementation (the other half of runtime.Object)
// will implement this by default.
//
// For example, nearly all the built-in types are Objects, as well as all
// KubeBuilder-generated CRDs (unless you do something real funky to them).
//
// By and large, most things that implement runtime.Object also implement
// Object -- it's very rare to have *just* a runtime.Object implementation (the
// cases tend to be funky built-in types like Webhook payloads that don't have
// a `metadata` field).
//
// Notice that XYZList types are distinct: they implement ObjectList instead.
type Object interface {
	metav1.Object
	runtime.Object
}

// ObjectList is a Kubernetes object list, allows functions to work
// indistinctly with any resource that implements both runtime.Object and
// metav1.ListInterface interfaces.
//
// Semantically, this is any object which may be serialized (ObjectMeta), and
// is a kubernetes list wrapper (has items, pagination fields, etc) -- think
// the wrapper used in a response from a `kubectl list --output yaml` call.
//
// Code-wise, this means that any object which embedds both ListMeta (which
// provides metav1.ListInterface) and TypeMeta (which provides half of
// runtime.Object) and has a `DeepCopyObject` implementation (the other half of
// runtime.Object) will implement this by default.
//
// For example, nearly all the built-in XYZList types are ObjectLists, as well
// as the XYZList types for all KubeBuilder-generated CRDs (unless you do
// something real funky to them).
//
// By and large, most things that are XYZList and implement runtime.Object also
// implement ObjectList -- it's very rare to have *just* a runtime.Object
// implementation (the cases tend to be funky built-in types like Webhook
// payloads that don't have a `metadata` field).
//
// This is similar to Object, which is almost always implemented by the items
// in the list themselves.
type ObjectList interface {
	metav1.ListInterface
	runtime.Object
}
