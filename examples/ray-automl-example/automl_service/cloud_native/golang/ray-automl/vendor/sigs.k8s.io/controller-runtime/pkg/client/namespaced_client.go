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

	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/internal/objectutil"
)

// NewNamespacedClient wraps an existing client enforcing the namespace value.
// All functions using this client will have the same namespace declared here.
func NewNamespacedClient(c Client, ns string) Client {
	return &namespacedClient{
		client:    c,
		namespace: ns,
	}
}

var _ Client = &namespacedClient{}

// namespacedClient is a Client that wraps another Client in order to enforce the specified namespace value.
type namespacedClient struct {
	namespace string
	client    Client
}

// Scheme returns the scheme this client is using.
func (n *namespacedClient) Scheme() *runtime.Scheme {
	return n.client.Scheme()
}

// RESTMapper returns the scheme this client is using.
func (n *namespacedClient) RESTMapper() meta.RESTMapper {
	return n.client.RESTMapper()
}

// Create implements client.Client.
func (n *namespacedClient) Create(ctx context.Context, obj Object, opts ...CreateOption) error {
	isNamespaceScoped, err := objectutil.IsAPINamespaced(obj, n.Scheme(), n.RESTMapper())
	if err != nil {
		return fmt.Errorf("error finding the scope of the object: %w", err)
	}

	objectNamespace := obj.GetNamespace()
	if objectNamespace != n.namespace && objectNamespace != "" {
		return fmt.Errorf("namespace %s of the object %s does not match the namespace %s on the client", objectNamespace, obj.GetName(), n.namespace)
	}

	if isNamespaceScoped && objectNamespace == "" {
		obj.SetNamespace(n.namespace)
	}
	return n.client.Create(ctx, obj, opts...)
}

// Update implements client.Client.
func (n *namespacedClient) Update(ctx context.Context, obj Object, opts ...UpdateOption) error {
	isNamespaceScoped, err := objectutil.IsAPINamespaced(obj, n.Scheme(), n.RESTMapper())
	if err != nil {
		return fmt.Errorf("error finding the scope of the object: %w", err)
	}

	objectNamespace := obj.GetNamespace()
	if objectNamespace != n.namespace && objectNamespace != "" {
		return fmt.Errorf("namespace %s of the object %s does not match the namespace %s on the client", objectNamespace, obj.GetName(), n.namespace)
	}

	if isNamespaceScoped && objectNamespace == "" {
		obj.SetNamespace(n.namespace)
	}
	return n.client.Update(ctx, obj, opts...)
}

// Delete implements client.Client.
func (n *namespacedClient) Delete(ctx context.Context, obj Object, opts ...DeleteOption) error {
	isNamespaceScoped, err := objectutil.IsAPINamespaced(obj, n.Scheme(), n.RESTMapper())
	if err != nil {
		return fmt.Errorf("error finding the scope of the object: %w", err)
	}

	objectNamespace := obj.GetNamespace()
	if objectNamespace != n.namespace && objectNamespace != "" {
		return fmt.Errorf("namespace %s of the object %s does not match the namespace %s on the client", objectNamespace, obj.GetName(), n.namespace)
	}

	if isNamespaceScoped && objectNamespace == "" {
		obj.SetNamespace(n.namespace)
	}
	return n.client.Delete(ctx, obj, opts...)
}

// DeleteAllOf implements client.Client.
func (n *namespacedClient) DeleteAllOf(ctx context.Context, obj Object, opts ...DeleteAllOfOption) error {
	isNamespaceScoped, err := objectutil.IsAPINamespaced(obj, n.Scheme(), n.RESTMapper())
	if err != nil {
		return fmt.Errorf("error finding the scope of the object: %w", err)
	}

	if isNamespaceScoped {
		opts = append(opts, InNamespace(n.namespace))
	}
	return n.client.DeleteAllOf(ctx, obj, opts...)
}

// Patch implements client.Client.
func (n *namespacedClient) Patch(ctx context.Context, obj Object, patch Patch, opts ...PatchOption) error {
	isNamespaceScoped, err := objectutil.IsAPINamespaced(obj, n.Scheme(), n.RESTMapper())
	if err != nil {
		return fmt.Errorf("error finding the scope of the object: %w", err)
	}

	objectNamespace := obj.GetNamespace()
	if objectNamespace != n.namespace && objectNamespace != "" {
		return fmt.Errorf("namespace %s of the object %s does not match the namespace %s on the client", objectNamespace, obj.GetName(), n.namespace)
	}

	if isNamespaceScoped && objectNamespace == "" {
		obj.SetNamespace(n.namespace)
	}
	return n.client.Patch(ctx, obj, patch, opts...)
}

// Get implements client.Client.
func (n *namespacedClient) Get(ctx context.Context, key ObjectKey, obj Object, opts ...GetOption) error {
	isNamespaceScoped, err := objectutil.IsAPINamespaced(obj, n.Scheme(), n.RESTMapper())
	if err != nil {
		return fmt.Errorf("error finding the scope of the object: %w", err)
	}
	if isNamespaceScoped {
		if key.Namespace != "" && key.Namespace != n.namespace {
			return fmt.Errorf("namespace %s provided for the object %s does not match the namespace %s on the client", key.Namespace, obj.GetName(), n.namespace)
		}
		key.Namespace = n.namespace
	}
	return n.client.Get(ctx, key, obj, opts...)
}

// List implements client.Client.
func (n *namespacedClient) List(ctx context.Context, obj ObjectList, opts ...ListOption) error {
	if n.namespace != "" {
		opts = append(opts, InNamespace(n.namespace))
	}
	return n.client.List(ctx, obj, opts...)
}

// Status implements client.StatusClient.
func (n *namespacedClient) Status() StatusWriter {
	return &namespacedClientStatusWriter{StatusClient: n.client.Status(), namespace: n.namespace, namespacedclient: n}
}

// ensure namespacedClientStatusWriter implements client.StatusWriter.
var _ StatusWriter = &namespacedClientStatusWriter{}

type namespacedClientStatusWriter struct {
	StatusClient     StatusWriter
	namespace        string
	namespacedclient Client
}

// Update implements client.StatusWriter.
func (nsw *namespacedClientStatusWriter) Update(ctx context.Context, obj Object, opts ...UpdateOption) error {
	isNamespaceScoped, err := objectutil.IsAPINamespaced(obj, nsw.namespacedclient.Scheme(), nsw.namespacedclient.RESTMapper())

	if err != nil {
		return fmt.Errorf("error finding the scope of the object: %w", err)
	}

	objectNamespace := obj.GetNamespace()
	if objectNamespace != nsw.namespace && objectNamespace != "" {
		return fmt.Errorf("namespace %s of the object %s does not match the namespace %s on the client", objectNamespace, obj.GetName(), nsw.namespace)
	}

	if isNamespaceScoped && objectNamespace == "" {
		obj.SetNamespace(nsw.namespace)
	}
	return nsw.StatusClient.Update(ctx, obj, opts...)
}

// Patch implements client.StatusWriter.
func (nsw *namespacedClientStatusWriter) Patch(ctx context.Context, obj Object, patch Patch, opts ...PatchOption) error {
	isNamespaceScoped, err := objectutil.IsAPINamespaced(obj, nsw.namespacedclient.Scheme(), nsw.namespacedclient.RESTMapper())

	if err != nil {
		return fmt.Errorf("error finding the scope of the object: %w", err)
	}

	objectNamespace := obj.GetNamespace()
	if objectNamespace != nsw.namespace && objectNamespace != "" {
		return fmt.Errorf("namespace %s of the object %s does not match the namespace %s on the client", objectNamespace, obj.GetName(), nsw.namespace)
	}

	if isNamespaceScoped && objectNamespace == "" {
		obj.SetNamespace(nsw.namespace)
	}
	return nsw.StatusClient.Patch(ctx, obj, patch, opts...)
}
