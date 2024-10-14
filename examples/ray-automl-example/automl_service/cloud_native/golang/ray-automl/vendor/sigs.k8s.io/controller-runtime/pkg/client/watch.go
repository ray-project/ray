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
	"context"
	"strings"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/rest"
)

// NewWithWatch returns a new WithWatch.
func NewWithWatch(config *rest.Config, options Options) (WithWatch, error) {
	client, err := newClient(config, options)
	if err != nil {
		return nil, err
	}
	dynamicClient, err := dynamic.NewForConfig(config)
	if err != nil {
		return nil, err
	}
	return &watchingClient{client: client, dynamic: dynamicClient}, nil
}

type watchingClient struct {
	*client
	dynamic dynamic.Interface
}

func (w *watchingClient) Watch(ctx context.Context, list ObjectList, opts ...ListOption) (watch.Interface, error) {
	switch l := list.(type) {
	case *unstructured.UnstructuredList:
		return w.unstructuredWatch(ctx, l, opts...)
	case *metav1.PartialObjectMetadataList:
		return w.metadataWatch(ctx, l, opts...)
	default:
		return w.typedWatch(ctx, l, opts...)
	}
}

func (w *watchingClient) listOpts(opts ...ListOption) ListOptions {
	listOpts := ListOptions{}
	listOpts.ApplyOptions(opts)
	if listOpts.Raw == nil {
		listOpts.Raw = &metav1.ListOptions{}
	}
	listOpts.Raw.Watch = true

	return listOpts
}

func (w *watchingClient) metadataWatch(ctx context.Context, obj *metav1.PartialObjectMetadataList, opts ...ListOption) (watch.Interface, error) {
	gvk := obj.GroupVersionKind()
	gvk.Kind = strings.TrimSuffix(gvk.Kind, "List")

	listOpts := w.listOpts(opts...)

	resInt, err := w.client.metadataClient.getResourceInterface(gvk, listOpts.Namespace)
	if err != nil {
		return nil, err
	}

	return resInt.Watch(ctx, *listOpts.AsListOptions())
}

func (w *watchingClient) unstructuredWatch(ctx context.Context, obj *unstructured.UnstructuredList, opts ...ListOption) (watch.Interface, error) {
	gvk := obj.GroupVersionKind()
	gvk.Kind = strings.TrimSuffix(gvk.Kind, "List")

	r, err := w.client.unstructuredClient.cache.getResource(obj)
	if err != nil {
		return nil, err
	}

	listOpts := w.listOpts(opts...)

	if listOpts.Namespace != "" && r.isNamespaced() {
		return w.dynamic.Resource(r.mapping.Resource).Namespace(listOpts.Namespace).Watch(ctx, *listOpts.AsListOptions())
	}
	return w.dynamic.Resource(r.mapping.Resource).Watch(ctx, *listOpts.AsListOptions())
}

func (w *watchingClient) typedWatch(ctx context.Context, obj ObjectList, opts ...ListOption) (watch.Interface, error) {
	r, err := w.client.typedClient.cache.getResource(obj)
	if err != nil {
		return nil, err
	}

	listOpts := w.listOpts(opts...)

	return r.Get().
		NamespaceIfScoped(listOpts.Namespace, r.isNamespaced()).
		Resource(r.resource()).
		VersionedParams(listOpts.AsListOptions(), w.client.typedClient.paramCodec).
		Watch(ctx)
}
