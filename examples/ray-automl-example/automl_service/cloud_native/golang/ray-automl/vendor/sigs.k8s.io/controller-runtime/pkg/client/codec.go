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

package client

import (
	"errors"
	"net/url"

	"k8s.io/apimachinery/pkg/conversion/queryparams"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
)

var _ runtime.ParameterCodec = noConversionParamCodec{}

// noConversionParamCodec is a no-conversion codec for serializing parameters into URL query strings.
// it's useful in scenarios with the unstructured client and arbitrary resources.
type noConversionParamCodec struct{}

func (noConversionParamCodec) EncodeParameters(obj runtime.Object, to schema.GroupVersion) (url.Values, error) {
	return queryparams.Convert(obj)
}

func (noConversionParamCodec) DecodeParameters(parameters url.Values, from schema.GroupVersion, into runtime.Object) error {
	return errors.New("DecodeParameters not implemented on noConversionParamCodec")
}
