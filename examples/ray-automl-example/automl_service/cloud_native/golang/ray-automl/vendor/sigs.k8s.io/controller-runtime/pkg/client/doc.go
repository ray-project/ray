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

// Package client contains functionality for interacting with Kubernetes API
// servers.
//
// # Clients
//
// Clients are split into two interfaces -- Readers and Writers.   Readers
// get and list, while writers create, update, and delete.
//
// The New function can be used to create a new client that talks directly
// to the API server.
//
// It is a common pattern in Kubernetes to read from a cache and write to the API
// server.  This pattern is covered by the DelegatingClient type, which can
// be used to have a client whose Reader is different from the Writer.
//
// # Options
//
// Many client operations in Kubernetes support options.  These options are
// represented as variadic arguments at the end of a given method call.
// For instance, to use a label selector on list, you can call
//
//	err := someReader.List(context.Background(), &podList, client.MatchingLabels{"somelabel": "someval"})
//
// # Indexing
//
// Indexes may be added to caches using a FieldIndexer.  This allows you to easily
// and efficiently look up objects with certain properties.  You can then make
// use of the index by specifying a field selector on calls to List on the Reader
// corresponding to the given Cache.
//
// For instance, a Secret controller might have an index on the
// `.spec.volumes.secret.secretName` field in Pod objects, so that it could
// easily look up all pods that reference a given secret.
package client
