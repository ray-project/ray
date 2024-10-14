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

package internal

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/fields"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime/schema"
)

// SelectorsByGVK associate a GroupVersionKind to a field/label selector.
type SelectorsByGVK map[schema.GroupVersionKind]Selector

func (s SelectorsByGVK) forGVK(gvk schema.GroupVersionKind) Selector {
	if specific, found := s[gvk]; found {
		return specific
	}
	if defaultSelector, found := s[schema.GroupVersionKind{}]; found {
		return defaultSelector
	}

	return Selector{}
}

// Selector specify the label/field selector to fill in ListOptions.
type Selector struct {
	Label labels.Selector
	Field fields.Selector
}

// ApplyToList fill in ListOptions LabelSelector and FieldSelector if needed.
func (s Selector) ApplyToList(listOpts *metav1.ListOptions) {
	if s.Label != nil {
		listOpts.LabelSelector = s.Label.String()
	}
	if s.Field != nil {
		listOpts.FieldSelector = s.Field.String()
	}
}
