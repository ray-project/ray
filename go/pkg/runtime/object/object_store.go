// Copyright 2025 The Ray Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//  http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package object

import (
	"context"

	"github.com/ray-project/ray/go/pkg/ids"
)

// WaitOptions holds options for the Wait operation.
// This structure encapsulates the parameters for waiting on objects to become ready.
type WaitOptions struct {
	// ObjectIDs is the list of object IDs to wait for.
	ObjectIDs []*ids.ObjectID
	// NumObjects is the number of objects to wait for.
	NumObjects int
	// TimeoutMs is the timeout in milliseconds.
	TimeoutMs int64
	// FetchLocal indicates whether to fetch local objects.
	FetchLocal bool
}

// ObjectStore is the interface for object storage.
// Implemented by NativeObjectStore.
//
// Designed to be compatible with Java's io.ray.runtime.object.ObjectStore.
type ObjectStore interface {
	// PutRaw stores the object and returns the generated ObjectID.
	PutRaw(obj *NativeRayObject) (*ids.ObjectID, error)

	// PutRawWithOwner stores the object with the specified owner actor ID.
	PutRawWithOwner(obj *NativeRayObject, ownerActorID *ids.ActorID) (*ids.ObjectID, error)

	// PutRawWithID stores the object with the specified ObjectID.
	PutRawWithID(obj *NativeRayObject, objectID *ids.ObjectID) error

	// GetRaw retrieves objects by their IDs with type information.
	// The objectType is used for type-safe deserialization, similar to Java's ObjectStore.get(objectType).
	GetRaw(objectIDs []*ids.ObjectID, timeoutMs int64, objectType string) ([]*NativeRayObject, error)

	// GetRawWithContext retrieves objects by their IDs with context support and type information.
	// The context can be used to cancel the operation or set a timeout.
	// The objectType is used for type-safe deserialization.
	GetRawWithContext(ctx context.Context, objectIDs []*ids.ObjectID, timeoutMs int64, objectType string) ([]*NativeRayObject, error)

	// Wait waits for objects to become ready.
	//
	// Deprecated: Use WaitWithOptions instead for better parameter organization.
	Wait(objectIDs []*ids.ObjectID, numObjects int, timeoutMs int64, fetchLocal bool) ([]bool, error)

	// WaitWithOptions waits for objects to become ready using the provided options.
	WaitWithOptions(opts WaitOptions) ([]bool, error)

	// Delete deletes objects from the object store.
	Delete(objectIDs []*ids.ObjectID, localOnly bool) error

	// AddLocalReference adds a local reference to the object.
	AddLocalReference(objectID *ids.ObjectID) error

	// RemoveLocalReference removes a local reference from the object.
	RemoveLocalReference(objectID *ids.ObjectID) error

	// GetOwnershipInfo returns the ownership info for the object.
	GetOwnershipInfo(objectID *ids.ObjectID) ([]byte, error)

	// RegisterOwnershipInfoAndResolveFuture registers ownership info and resolves future.
	RegisterOwnershipInfoAndResolveFuture(objectID, outerObjectID *ids.ObjectID, ownerAddress []byte) error

	// GetOwnerAddress returns the owner address of the object.
	GetOwnerAddress(objectID *ids.ObjectID) ([]byte, error)

	// GetAllReferenceCounts returns all reference counts.
	GetAllReferenceCounts() (map[ids.ObjectID][2]int64, error)
}
