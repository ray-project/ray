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

/*
Package conversion provides interface definitions that an API Type needs to
implement for it to be supported by the generic conversion webhook handler
defined under pkg/webhook/conversion.
*/
package conversion

import "k8s.io/apimachinery/pkg/runtime"

// Convertible defines capability of a type to convertible i.e. it can be converted to/from a hub type.
type Convertible interface {
	runtime.Object
	ConvertTo(dst Hub) error
	ConvertFrom(src Hub) error
}

// Hub marks that a given type is the hub type for conversion. This means that
// all conversions will first convert to the hub type, then convert from the hub
// type to the destination type. All types besides the hub type should implement
// Convertible.
type Hub interface {
	runtime.Object
	Hub()
}
