/*
Copyright 2021 The Kubernetes Authors.

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

package envtest

import (
	apiextensionsv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	"k8s.io/client-go/kubernetes/scheme"
)

var (
	crdScheme = scheme.Scheme
)

// init is required to correctly initialize the crdScheme package variable.
func init() {
	_ = apiextensionsv1.AddToScheme(crdScheme)
}

// mergePaths merges two string slices containing paths.
// This function makes no guarantees about order of the merged slice.
func mergePaths(s1, s2 []string) []string {
	m := make(map[string]struct{})
	for _, s := range s1 {
		m[s] = struct{}{}
	}
	for _, s := range s2 {
		m[s] = struct{}{}
	}
	merged := make([]string, len(m))
	i := 0
	for key := range m {
		merged[i] = key
		i++
	}
	return merged
}

// mergeCRDs merges two CRD slices using their names.
// This function makes no guarantees about order of the merged slice.
func mergeCRDs(s1, s2 []*apiextensionsv1.CustomResourceDefinition) []*apiextensionsv1.CustomResourceDefinition {
	m := make(map[string]*apiextensionsv1.CustomResourceDefinition)
	for _, obj := range s1 {
		m[obj.GetName()] = obj
	}
	for _, obj := range s2 {
		m[obj.GetName()] = obj
	}
	merged := make([]*apiextensionsv1.CustomResourceDefinition, len(m))
	i := 0
	for _, obj := range m {
		merged[i] = obj.DeepCopy()
		i++
	}
	return merged
}
