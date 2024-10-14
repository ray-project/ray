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

// Package apiutil contains utilities for working with raw Kubernetes
// API machinery, such as creating RESTMappers and raw REST clients,
// and extracting the GVK of an object.
package apiutil

import (
	"fmt"
	"reflect"
	"sync"

	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/runtime/serializer"
	"k8s.io/client-go/discovery"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/restmapper"
)

var (
	protobufScheme     = runtime.NewScheme()
	protobufSchemeLock sync.RWMutex
)

func init() {
	// Currently only enabled for built-in resources which are guaranteed to implement Protocol Buffers.
	// For custom resources, CRDs can not support Protocol Buffers but Aggregated API can.
	// See doc: https://kubernetes.io/docs/concepts/extend-kubernetes/api-extension/custom-resources/#advanced-features-and-flexibility
	if err := clientgoscheme.AddToScheme(protobufScheme); err != nil {
		panic(err)
	}
}

// AddToProtobufScheme add the given SchemeBuilder into protobufScheme, which should
// be additional types that do support protobuf.
func AddToProtobufScheme(addToScheme func(*runtime.Scheme) error) error {
	protobufSchemeLock.Lock()
	defer protobufSchemeLock.Unlock()
	return addToScheme(protobufScheme)
}

// NewDiscoveryRESTMapper constructs a new RESTMapper based on discovery
// information fetched by a new client with the given config.
func NewDiscoveryRESTMapper(c *rest.Config) (meta.RESTMapper, error) {
	// Get a mapper
	dc, err := discovery.NewDiscoveryClientForConfig(c)
	if err != nil {
		return nil, err
	}
	gr, err := restmapper.GetAPIGroupResources(dc)
	if err != nil {
		return nil, err
	}
	return restmapper.NewDiscoveryRESTMapper(gr), nil
}

// GVKForObject finds the GroupVersionKind associated with the given object, if there is only a single such GVK.
func GVKForObject(obj runtime.Object, scheme *runtime.Scheme) (schema.GroupVersionKind, error) {
	// TODO(directxman12): do we want to generalize this to arbitrary container types?
	// I think we'd need a generalized form of scheme or something.  It's a
	// shame there's not a reliable "GetGVK" interface that works by default
	// for unpopulated static types and populated "dynamic" types
	// (unstructured, partial, etc)

	// check for PartialObjectMetadata, which is analogous to unstructured, but isn't handled by ObjectKinds
	_, isPartial := obj.(*metav1.PartialObjectMetadata) //nolint:ifshort
	_, isPartialList := obj.(*metav1.PartialObjectMetadataList)
	if isPartial || isPartialList {
		// we require that the GVK be populated in order to recognize the object
		gvk := obj.GetObjectKind().GroupVersionKind()
		if len(gvk.Kind) == 0 {
			return schema.GroupVersionKind{}, runtime.NewMissingKindErr("unstructured object has no kind")
		}
		if len(gvk.Version) == 0 {
			return schema.GroupVersionKind{}, runtime.NewMissingVersionErr("unstructured object has no version")
		}
		return gvk, nil
	}

	gvks, isUnversioned, err := scheme.ObjectKinds(obj)
	if err != nil {
		return schema.GroupVersionKind{}, err
	}
	if isUnversioned {
		return schema.GroupVersionKind{}, fmt.Errorf("cannot create group-version-kind for unversioned type %T", obj)
	}

	if len(gvks) < 1 {
		return schema.GroupVersionKind{}, fmt.Errorf("no group-version-kinds associated with type %T", obj)
	}
	if len(gvks) > 1 {
		// this should only trigger for things like metav1.XYZ --
		// normal versioned types should be fine
		return schema.GroupVersionKind{}, fmt.Errorf(
			"multiple group-version-kinds associated with type %T, refusing to guess at one", obj)
	}
	return gvks[0], nil
}

// RESTClientForGVK constructs a new rest.Interface capable of accessing the resource associated
// with the given GroupVersionKind. The REST client will be configured to use the negotiated serializer from
// baseConfig, if set, otherwise a default serializer will be set.
func RESTClientForGVK(gvk schema.GroupVersionKind, isUnstructured bool, baseConfig *rest.Config, codecs serializer.CodecFactory) (rest.Interface, error) {
	return rest.RESTClientFor(createRestConfig(gvk, isUnstructured, baseConfig, codecs))
}

// serializerWithDecodedGVK is a CodecFactory that overrides the DecoderToVersion of a WithoutConversionCodecFactory
// in order to avoid clearing the GVK from the decoded object.
//
// See https://github.com/kubernetes/kubernetes/issues/80609.
type serializerWithDecodedGVK struct {
	serializer.WithoutConversionCodecFactory
}

// DecoderToVersion returns an decoder that does not do conversion.
func (f serializerWithDecodedGVK) DecoderToVersion(serializer runtime.Decoder, _ runtime.GroupVersioner) runtime.Decoder {
	return serializer
}

// createRestConfig copies the base config and updates needed fields for a new rest config.
func createRestConfig(gvk schema.GroupVersionKind, isUnstructured bool, baseConfig *rest.Config, codecs serializer.CodecFactory) *rest.Config {
	gv := gvk.GroupVersion()

	cfg := rest.CopyConfig(baseConfig)
	cfg.GroupVersion = &gv
	if gvk.Group == "" {
		cfg.APIPath = "/api"
	} else {
		cfg.APIPath = "/apis"
	}
	if cfg.UserAgent == "" {
		cfg.UserAgent = rest.DefaultKubernetesUserAgent()
	}
	// TODO(FillZpp): In the long run, we want to check discovery or something to make sure that this is actually true.
	if cfg.ContentType == "" && !isUnstructured {
		protobufSchemeLock.RLock()
		if protobufScheme.Recognizes(gvk) {
			cfg.ContentType = runtime.ContentTypeProtobuf
		}
		protobufSchemeLock.RUnlock()
	}

	if isUnstructured {
		// If the object is unstructured, we need to preserve the GVK information.
		// Use our own custom serializer.
		cfg.NegotiatedSerializer = serializerWithDecodedGVK{serializer.WithoutConversionCodecFactory{CodecFactory: codecs}}
	} else {
		cfg.NegotiatedSerializer = serializerWithTargetZeroingDecode{NegotiatedSerializer: serializer.WithoutConversionCodecFactory{CodecFactory: codecs}}
	}

	return cfg
}

type serializerWithTargetZeroingDecode struct {
	runtime.NegotiatedSerializer
}

func (s serializerWithTargetZeroingDecode) DecoderToVersion(serializer runtime.Decoder, r runtime.GroupVersioner) runtime.Decoder {
	return targetZeroingDecoder{upstream: s.NegotiatedSerializer.DecoderToVersion(serializer, r)}
}

type targetZeroingDecoder struct {
	upstream runtime.Decoder
}

func (t targetZeroingDecoder) Decode(data []byte, defaults *schema.GroupVersionKind, into runtime.Object) (runtime.Object, *schema.GroupVersionKind, error) {
	zero(into)
	return t.upstream.Decode(data, defaults, into)
}

// zero zeros the value of a pointer.
func zero(x interface{}) {
	if x == nil {
		return
	}
	res := reflect.ValueOf(x).Elem()
	res.Set(reflect.Zero(res.Type()))
}
