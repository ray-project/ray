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
	"fmt"

	"github.com/ray-project/ray/go/pkg/ids"
	"github.com/vmihailenco/msgpack/v5"
)

// NativeActorHandle represents a cross-language Actor handle.
// Compatible with Java NativeActorHandle and C++ ActorHandle.
type NativeActorHandle struct {
	// ActorID is the unique identifier for the actor.
	ActorID ids.ActorID

	// Language is the owner language (GO, JAVA, PYTHON, CPP).
	Language Language

	// OwnerAddress is the owner address for cross-node calls.
	OwnerAddress *ActorOwnerAddress

	// ActorHandleID is the handle ID for reference tracking (consistent with Java).
	ActorHandleID ids.ObjectID

	// Serialized contains extra serialization data if needed.
	Serialized []byte
}

// ActorOwnerAddress represents the owner address information for an Actor.
type ActorOwnerAddress struct {
	IPAddress string `msgpack:"ip"` // Note: Using "ip" in msgpack tag for cross-language compatibility
	Port      int    `msgpack:"port"`
	WorkerID  string `msgpack:"worker_id"`
}

// ID returns the actor ID.
// This method implements the submitter.ActorHandle interface.
func (h *NativeActorHandle) ID() ids.ActorID {
	return h.ActorID
}

// Serialize serializes the ActorHandle.
// Returns a NativeRayObject with ACTOR_HANDLE metadata.
func (h *NativeActorHandle) Serialize() (*NativeRayObject, error) {
	if h.ActorID.IsNil() {
		return nil, fmt.Errorf("ActorID cannot be nil")
	}

	// Build serializable data structure.
	handleData := map[string]interface{}{
		"actor_id":        h.ActorID.Binary(),
		"language":        h.Language,
		"actor_handle_id": h.ActorHandleID.Binary(),
	}

	// Add owner address if present.
	if h.OwnerAddress != nil {
		handleData["owner_address"] = h.OwnerAddress
	}

	// Serialize using MsgPack.
	data, err := msgpack.Marshal(handleData)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal ActorHandle: %w", err)
	}

	nativeObj := &NativeRayObject{
		Data:     data,
		Metadata: []byte(MetadataTypeActorHandle),
	}

	// Set contained object IDs for dependency tracking.
	// Consistent with Java implementation, set ActorHandleID as the contained object.
	nativeObj.SetContainedObjectIds([]ids.ObjectID{h.ActorHandleID})

	return nativeObj, nil
}

// DeserializeActorHandle deserializes an ActorHandle.
// Restores an ActorHandle object from a NativeRayObject.
func DeserializeActorHandle(nativeObj *NativeRayObject) (*NativeActorHandle, error) {
	if nativeObj == nil {
		return nil, fmt.Errorf("NativeRayObject is nil")
	}

	if string(nativeObj.Metadata) != MetadataTypeActorHandle {
		return nil, fmt.Errorf(
			"invalid metadata type: expected %s, got %s",
			MetadataTypeActorHandle, string(nativeObj.Metadata))
	}

	// Deserialize data.
	var handleData map[string]interface{}
	if err := msgpack.Unmarshal(nativeObj.Data, &handleData); err != nil {
		return nil, fmt.Errorf("failed to unmarshal ActorHandle: %w", err)
	}

	// Extract ActorID.
	actorIDBytes, ok := handleData["actor_id"].([]byte)
	if !ok {
		return nil, fmt.Errorf("missing or invalid actor_id in serialized data")
	}
	actorID, err := ids.ActorIDFromBinary(actorIDBytes)
	if err != nil {
		return nil, fmt.Errorf("failed to parse ActorID: %w", err)
	}

	// Extract Language.
	languageStr, ok := handleData["language"].(string)
	if !ok {
		return nil, fmt.Errorf("missing or invalid language in serialized data")
	}
	language := Language(languageStr)

	// Extract ActorHandleID.
	var actorHandleID ids.ObjectID
	if handleIDBytes, ok := handleData["actor_handle_id"].([]byte); ok {
		actorHandleID, err = ids.ObjectIDFromBinary(handleIDBytes)
		if err != nil {
			return nil, fmt.Errorf("failed to parse ActorHandleID: %w", err)
		}
	}

	// Build NativeActorHandle.
	handle := &NativeActorHandle{
		ActorID:       actorID,
		Language:      language,
		ActorHandleID: actorHandleID,
	}

	// Extract OwnerAddress if present.
	if ownerData, ok := handleData["owner_address"].(map[string]interface{}); ok {
		ip, _ := ownerData["ip"].(string)
		// MsgPack may decode numbers to different integer types, handle flexibly.
		var port int
		switch p := ownerData["port"].(type) {
		case int64:
			port = int(p)
		case uint16:
			port = int(p)
		case int:
			port = p
		default:
			port = 0
		}
		workerID, _ := ownerData["worker_id"].(string)

		handle.OwnerAddress = &ActorOwnerAddress{
			IPAddress: ip,
			Port:      port,
			WorkerID:  workerID,
		}
	}

	return handle, nil
}
