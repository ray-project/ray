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

// Package scheme contains utilities for gradually building Schemes,
// which contain information associating Go types with Kubernetes
// groups, versions, and kinds.
//
// Each API group should define a utility function
// called AddToScheme for adding its types to a Scheme:
//
//	 // in package myapigroupv1...
//	var (
//		SchemeGroupVersion = schema.GroupVersion{Group: "my.api.group", Version: "v1"}
//		SchemeBuilder = &scheme.Builder{GroupVersion: SchemeGroupVersion}
//		AddToScheme = SchemeBuilder.AddToScheme
//	)
//
//	func init() {
//		SchemeBuilder.Register(&MyType{}, &MyTypeList)
//	}
//	var (
//		scheme *runtime.Scheme = runtime.NewScheme()
//	)
//
// This also true of the built-in Kubernetes types.  Then, in the entrypoint for
// your manager, assemble the scheme containing exactly the types you need,
// panicing if scheme registration failed. For instance, if our controller needs
// types from the core/v1 API group (e.g. Pod), plus types from my.api.group/v1:
//
//	func init() {
//		utilruntime.Must(myapigroupv1.AddToScheme(scheme))
//		utilruntime.Must(kubernetesscheme.AddToScheme(scheme))
//	}
//
//	func main() {
//		mgr := controllers.NewManager(context.Background(), controllers.GetConfigOrDie(), manager.Options{
//			Scheme: scheme,
//		})
//		// ...
//	}
package scheme

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
)

// Builder builds a new Scheme for mapping go types to Kubernetes GroupVersionKinds.
type Builder struct {
	GroupVersion schema.GroupVersion
	runtime.SchemeBuilder
}

// Register adds one or more objects to the SchemeBuilder so they can be added to a Scheme.  Register mutates bld.
func (bld *Builder) Register(object ...runtime.Object) *Builder {
	bld.SchemeBuilder.Register(func(scheme *runtime.Scheme) error {
		scheme.AddKnownTypes(bld.GroupVersion, object...)
		metav1.AddToGroupVersion(scheme, bld.GroupVersion)
		return nil
	})
	return bld
}

// RegisterAll registers all types from the Builder argument.  RegisterAll mutates bld.
func (bld *Builder) RegisterAll(b *Builder) *Builder {
	bld.SchemeBuilder = append(bld.SchemeBuilder, b.SchemeBuilder...)
	return bld
}

// AddToScheme adds all registered types to s.
func (bld *Builder) AddToScheme(s *runtime.Scheme) error {
	return bld.SchemeBuilder.AddToScheme(s)
}

// Build returns a new Scheme containing the registered types.
func (bld *Builder) Build() (*runtime.Scheme, error) {
	s := runtime.NewScheme()
	return s, bld.AddToScheme(s)
}
