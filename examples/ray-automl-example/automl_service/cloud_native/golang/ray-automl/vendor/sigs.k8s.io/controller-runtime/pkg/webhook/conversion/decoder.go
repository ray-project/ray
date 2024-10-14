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

package conversion

import (
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/runtime/serializer"
)

// Decoder knows how to decode the contents of a CRD version conversion
// request into a concrete object.
// TODO(droot): consider reusing decoder from admission pkg for this.
type Decoder struct {
	codecs serializer.CodecFactory
}

// NewDecoder creates a Decoder given the runtime.Scheme
func NewDecoder(scheme *runtime.Scheme) (*Decoder, error) {
	return &Decoder{codecs: serializer.NewCodecFactory(scheme)}, nil
}

// Decode decodes the inlined object.
func (d *Decoder) Decode(content []byte) (runtime.Object, *schema.GroupVersionKind, error) {
	deserializer := d.codecs.UniversalDeserializer()
	return deserializer.Decode(content, nil, nil)
}

// DecodeInto decodes the inlined object in the into the passed-in runtime.Object.
func (d *Decoder) DecodeInto(content []byte, into runtime.Object) error {
	deserializer := d.codecs.UniversalDeserializer()
	return runtime.DecodeInto(deserializer, content, into)
}
