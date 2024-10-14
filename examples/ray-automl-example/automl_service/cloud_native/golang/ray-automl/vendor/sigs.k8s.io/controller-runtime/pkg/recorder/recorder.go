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

// Package recorder defines interfaces for working with Kubernetes event recorders.
//
// You can use these to emit Kubernetes events associated with a particular Kubernetes
// object.
package recorder

import (
	"k8s.io/client-go/tools/record"
)

// Provider knows how to generate new event recorders with given name.
type Provider interface {
	// NewRecorder returns an EventRecorder with given name.
	GetEventRecorderFor(name string) record.EventRecorder
}
