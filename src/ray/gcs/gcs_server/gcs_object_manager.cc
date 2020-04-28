// Copyright 2017 The Ray Authors.
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

#include "gcs_object_manager.h"
#include "ray/gcs/pb_util.h"
#include "ray/util/logging.h"

namespace ray {

namespace gcs {

void GcsObjectManager::HandleGetObjectLocations(
    const rpc::GetObjectLocationsRequest &request, rpc::GetObjectLocationsReply *reply,
    rpc::SendReplyCallback send_reply_callback) {
  ObjectID object_id = ObjectID::FromBinary(request.object_id());
  RAY_LOG(DEBUG) << "Getting object locations, job id = " << object_id.TaskId().JobId()
                 << ", object id = " << object_id;
  auto object_locations = GetObjectLocations(object_id);
  for (auto &node_id : object_locations) {
    rpc::ObjectTableData object_table_data;
    object_table_data.set_manager(node_id.Binary());
    reply->add_object_table_data_list()->CopyFrom(object_table_data);
  }
  RAY_LOG(DEBUG) << "Finished getting object locations, job id = "
                 << object_id.TaskId().JobId() << ", object id = " << object_id;
  GCS_RPC_SEND_REPLY(send_reply_callback, reply, Status::OK());
}

void GcsObjectManager::HandleAddObjectLocation(
    const rpc::AddObjectLocationRequest &request, rpc::AddObjectLocationReply *reply,
    rpc::SendReplyCallback send_reply_callback) {
  ObjectID object_id = ObjectID::FromBinary(request.object_id());
  ClientID node_id = ClientID::FromBinary(request.node_id());
  RAY_LOG(DEBUG) << "Adding object location, job id = " << object_id.TaskId().JobId()
                 << ", object id = " << object_id << ", node id = " << node_id;
  AddObjectLocation(object_id, node_id);
  RAY_CHECK_OK(gcs_pub_sub_->Publish(
      OBJECT_CHANNEL, object_id.Binary(),
      gcs::CreateObjectLocationChange(node_id, true)->SerializeAsString(), nullptr));
  RAY_LOG(DEBUG) << "Finished adding object location, job id = "
                 << object_id.TaskId().JobId() << ", object id = " << object_id
                 << ", node id = " << node_id;
  GCS_RPC_SEND_REPLY(send_reply_callback, reply, Status::OK());
}

void GcsObjectManager::HandleRemoveObjectLocation(
    const rpc::RemoveObjectLocationRequest &request,
    rpc::RemoveObjectLocationReply *reply, rpc::SendReplyCallback send_reply_callback) {
  ObjectID object_id = ObjectID::FromBinary(request.object_id());
  ClientID node_id = ClientID::FromBinary(request.node_id());
  RAY_LOG(DEBUG) << "Removing object location, job id = " << object_id.TaskId().JobId()
                 << ", object id = " << object_id << ", node id = " << node_id;
  RemoveObjectLocation(object_id, node_id);
  RAY_CHECK_OK(gcs_pub_sub_->Publish(
      OBJECT_CHANNEL, object_id.Binary(),
      gcs::CreateObjectLocationChange(node_id, false)->SerializeAsString(), nullptr));
  RAY_LOG(DEBUG) << "Finished removing object location, job id = "
                 << object_id.TaskId().JobId() << ", object id = " << object_id
                 << ", node id = " << node_id;
  GCS_RPC_SEND_REPLY(send_reply_callback, reply, Status::OK());
}

void GcsObjectManager::AddObjectsLocation(
    const ClientID &node_id, const std::unordered_set<ObjectID> &object_ids) {
  // TODO(micafan) Optimize the lock when necessary.
  // Maybe use read/write lock. Or reduce the granularity of the lock.
  absl::MutexLock lock(&mutex_);

  auto *node_hold_objects = GetNodeHoldObjectSet(node_id, /* create_if_not_exist */ true);
  node_hold_objects->insert(object_ids.begin(), object_ids.end());

  for (const auto &object_id : object_ids) {
    auto *object_locations =
        GetObjectLocationSet(object_id, /* create_if_not_exist */ true);
    object_locations->emplace(node_id);
  }
}

void GcsObjectManager::AddObjectLocation(const ObjectID &object_id,
                                         const ClientID &node_id) {
  absl::MutexLock lock(&mutex_);

  auto *node_hold_objects = GetNodeHoldObjectSet(node_id, /* create_if_not_exist */ true);
  node_hold_objects->emplace(object_id);

  auto *object_locations =
      GetObjectLocationSet(object_id, /* create_if_not_exist */ true);
  object_locations->emplace(node_id);
}

std::unordered_set<ClientID> GcsObjectManager::GetObjectLocations(
    const ObjectID &object_id) {
  absl::MutexLock lock(&mutex_);

  auto *object_locations = GetObjectLocationSet(object_id);
  if (object_locations) {
    return *object_locations;
  }
  return std::unordered_set<ClientID>{};
}

void GcsObjectManager::RemoveNode(const ClientID &node_id) {
  absl::MutexLock lock(&mutex_);

  ObjectSet node_hold_objects;
  auto it = node_to_objects_.find(node_id);
  if (it != node_to_objects_.end()) {
    node_hold_objects.swap(it->second);
    node_to_objects_.erase(it);
  }

  if (node_hold_objects.empty()) {
    return;
  }

  for (const auto &object_id : node_hold_objects) {
    auto *object_locations = GetObjectLocationSet(object_id);
    if (object_locations) {
      object_locations->erase(node_id);
      if (object_locations->empty()) {
        object_to_locations_.erase(object_id);
      }
    }
  }
}

void GcsObjectManager::RemoveObjectLocation(const ObjectID &object_id,
                                            const ClientID &node_id) {
  absl::MutexLock lock(&mutex_);

  auto *object_locations = GetObjectLocationSet(object_id);
  if (object_locations) {
    object_locations->erase(node_id);
    if (object_locations->empty()) {
      object_to_locations_.erase(object_id);
    }
  }

  auto *node_hold_objects = GetNodeHoldObjectSet(node_id);
  if (node_hold_objects) {
    node_hold_objects->erase(object_id);
    if (node_hold_objects->empty()) {
      node_to_objects_.erase(node_id);
    }
  }
}

GcsObjectManager::LocationSet *GcsObjectManager::GetObjectLocationSet(
    const ObjectID &object_id, bool create_if_not_exist) {
  LocationSet *object_locations = nullptr;

  auto it = object_to_locations_.find(object_id);
  if (it != object_to_locations_.end()) {
    object_locations = &it->second;
  } else if (create_if_not_exist) {
    auto ret = object_to_locations_.emplace(std::make_pair(object_id, LocationSet{}));
    RAY_CHECK(ret.second);
    object_locations = &(ret.first->second);
  }

  return object_locations;
}

GcsObjectManager::ObjectSet *GcsObjectManager::GetNodeHoldObjectSet(
    const ClientID &node_id, bool create_if_not_exist) {
  ObjectSet *node_hold_objects = nullptr;

  auto it = node_to_objects_.find(node_id);
  if (it != node_to_objects_.end()) {
    node_hold_objects = &it->second;
  } else if (create_if_not_exist) {
    auto ret = node_to_objects_.emplace(std::make_pair(node_id, ObjectSet{}));
    RAY_CHECK(ret.second);
    node_hold_objects = &(ret.first->second);
  }
  return node_hold_objects;
}

}  // namespace gcs

}  // namespace ray
