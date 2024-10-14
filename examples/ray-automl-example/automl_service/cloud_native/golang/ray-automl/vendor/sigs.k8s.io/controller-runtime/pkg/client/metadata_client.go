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
	"fmt"
	"strings"

	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/metadata"
)

// TODO(directxman12): we could rewrite this on top of the low-level REST
// client to avoid the extra shallow copy at the end, but I'm not sure it's
// worth it -- the metadata client deals with falling back to loading the whole
// object on older API servers, etc, and we'd have to reproduce that.

// metadataClient is a client that reads & writes metadata-only requests to/from the API server.
type metadataClient struct {
	client     metadata.Interface
	restMapper meta.RESTMapper
}

func (mc *metadataClient) getResourceInterface(gvk schema.GroupVersionKind, ns string) (metadata.ResourceInterface, error) {
	mapping, err := mc.restMapper.RESTMapping(gvk.GroupKind(), gvk.Version)
	if err != nil {
		return nil, err
	}
	if mapping.Scope.Name() == meta.RESTScopeNameRoot {
		return mc.client.Resource(mapping.Resource), nil
	}
	return mc.client.Resource(mapping.Resource).Namespace(ns), nil
}

// Delete implements client.Client.
func (mc *metadataClient) Delete(ctx context.Context, obj Object, opts ...DeleteOption) error {
	metadata, ok := obj.(*metav1.PartialObjectMetadata)
	if !ok {
		return fmt.Errorf("metadata client did not understand object: %T", obj)
	}

	resInt, err := mc.getResourceInterface(metadata.GroupVersionKind(), metadata.Namespace)
	if err != nil {
		return err
	}

	deleteOpts := DeleteOptions{}
	deleteOpts.ApplyOptions(opts)

	return resInt.Delete(ctx, metadata.Name, *deleteOpts.AsDeleteOptions())
}

// DeleteAllOf implements client.Client.
func (mc *metadataClient) DeleteAllOf(ctx context.Context, obj Object, opts ...DeleteAllOfOption) error {
	metadata, ok := obj.(*metav1.PartialObjectMetadata)
	if !ok {
		return fmt.Errorf("metadata client did not understand object: %T", obj)
	}

	deleteAllOfOpts := DeleteAllOfOptions{}
	deleteAllOfOpts.ApplyOptions(opts)

	resInt, err := mc.getResourceInterface(metadata.GroupVersionKind(), deleteAllOfOpts.ListOptions.Namespace)
	if err != nil {
		return err
	}

	return resInt.DeleteCollection(ctx, *deleteAllOfOpts.AsDeleteOptions(), *deleteAllOfOpts.AsListOptions())
}

// Patch implements client.Client.
func (mc *metadataClient) Patch(ctx context.Context, obj Object, patch Patch, opts ...PatchOption) error {
	metadata, ok := obj.(*metav1.PartialObjectMetadata)
	if !ok {
		return fmt.Errorf("metadata client did not understand object: %T", obj)
	}

	gvk := metadata.GroupVersionKind()
	resInt, err := mc.getResourceInterface(gvk, metadata.Namespace)
	if err != nil {
		return err
	}

	data, err := patch.Data(obj)
	if err != nil {
		return err
	}

	patchOpts := &PatchOptions{}
	patchOpts.ApplyOptions(opts)

	res, err := resInt.Patch(ctx, metadata.Name, patch.Type(), data, *patchOpts.AsPatchOptions())
	if err != nil {
		return err
	}
	*metadata = *res
	metadata.SetGroupVersionKind(gvk) // restore the GVK, which isn't set on metadata
	return nil
}

// Get implements client.Client.
func (mc *metadataClient) Get(ctx context.Context, key ObjectKey, obj Object, opts ...GetOption) error {
	metadata, ok := obj.(*metav1.PartialObjectMetadata)
	if !ok {
		return fmt.Errorf("metadata client did not understand object: %T", obj)
	}

	gvk := metadata.GroupVersionKind()

	getOpts := GetOptions{}
	getOpts.ApplyOptions(opts)

	resInt, err := mc.getResourceInterface(gvk, key.Namespace)
	if err != nil {
		return err
	}

	res, err := resInt.Get(ctx, key.Name, *getOpts.AsGetOptions())
	if err != nil {
		return err
	}
	*metadata = *res
	metadata.SetGroupVersionKind(gvk) // restore the GVK, which isn't set on metadata
	return nil
}

// List implements client.Client.
func (mc *metadataClient) List(ctx context.Context, obj ObjectList, opts ...ListOption) error {
	metadata, ok := obj.(*metav1.PartialObjectMetadataList)
	if !ok {
		return fmt.Errorf("metadata client did not understand object: %T", obj)
	}

	gvk := metadata.GroupVersionKind()
	gvk.Kind = strings.TrimSuffix(gvk.Kind, "List")

	listOpts := ListOptions{}
	listOpts.ApplyOptions(opts)

	resInt, err := mc.getResourceInterface(gvk, listOpts.Namespace)
	if err != nil {
		return err
	}

	res, err := resInt.List(ctx, *listOpts.AsListOptions())
	if err != nil {
		return err
	}
	*metadata = *res
	metadata.SetGroupVersionKind(gvk) // restore the GVK, which isn't set on metadata
	return nil
}

func (mc *metadataClient) PatchStatus(ctx context.Context, obj Object, patch Patch, opts ...PatchOption) error {
	metadata, ok := obj.(*metav1.PartialObjectMetadata)
	if !ok {
		return fmt.Errorf("metadata client did not understand object: %T", obj)
	}

	gvk := metadata.GroupVersionKind()
	resInt, err := mc.getResourceInterface(gvk, metadata.Namespace)
	if err != nil {
		return err
	}

	data, err := patch.Data(obj)
	if err != nil {
		return err
	}

	patchOpts := &PatchOptions{}
	res, err := resInt.Patch(ctx, metadata.Name, patch.Type(), data, *patchOpts.AsPatchOptions(), "status")
	if err != nil {
		return err
	}
	*metadata = *res
	metadata.SetGroupVersionKind(gvk) // restore the GVK, which isn't set on metadata
	return nil
}
