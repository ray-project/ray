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

#include "ray/gcs/gcs_server/gcs_object_manager.h"

#include "ray/gcs/pb_util.h"

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

void GcsObjectManager::HandleGetAllObjectLocations(
    const rpc::GetAllObjectLocationsRequest &request,
    rpc::GetAllObjectLocationsReply *reply, rpc::SendReplyCallback send_reply_callback) {
  RAY_LOG(DEBUG) << "Getting all object locations.";
  absl::MutexLock lock(&mutex_);
  for (auto &item : object_to_locations_) {
    rpc::ObjectLocationInfo object_location_info;
    object_location_info.set_object_id(item.first.Binary());
    for (auto &node_id : item.second) {
      rpc::ObjectTableData object_table_data;
      object_table_data.set_manager(node_id.Binary());
      object_location_info.add_locations()->CopyFrom(object_table_data);
    }
    reply->add_object_location_info_list()->CopyFrom(object_location_info);
  }
  RAY_LOG(DEBUG) << "Finished getting all object locations.";
  GCS_RPC_SEND_REPLY(send_reply_callback, reply, Status::OK());
}

void GcsObjectManager::HandleAddObjectLocation(
    const rpc::AddObjectLocationRequest &request, rpc::AddObjectLocationReply *reply,
    rpc::SendReplyCallback send_reply_callback) {
  ObjectID object_id = ObjectID::FromBinary(request.object_id());
  ClientID node_id = ClientID::FromBinary(request.node_id());
  RAY_LOG(DEBUG) << "Adding object location, job id = " << object_id.TaskId().JobId()
                 << ", object id = " << object_id << ", node id = " << node_id;
  AddObjectLocationInCache(object_id, node_id);

  auto on_done = [this, object_id, node_id, reply,
                  send_reply_callback](const Status &status) {
    if (status.ok()) {
      RAY_CHECK_OK(gcs_pub_sub_->Publish(
          OBJECT_CHANNEL, object_id.Hex(),
          gcs::CreateObjectLocationChange(node_id, true)->SerializeAsString(), nullptr));
      RAY_LOG(DEBUG) << "Finished adding object location, job id = "
                     << object_id.TaskId().JobId() << ", object id = " << object_id
                     << ", node id = " << node_id << ", task id = " << object_id.TaskId();
    } else {
      RAY_LOG(ERROR) << "Failed to add object location: " << status.ToString()
                     << ", job id = " << object_id.TaskId().JobId()
                     << ", object id = " << object_id << ", node id = " << node_id;
    }
    // We should only reply after the update is written to storage.
    // So, if GCS server crashes before writing storage, GCS client will retry this
    // request.
    GCS_RPC_SEND_REPLY(send_reply_callback, reply, status);
  };

  absl::MutexLock lock(&mutex_);
  auto object_location_set =
      GetObjectLocationSet(object_id, /* create_if_not_exist */ false);
  auto object_table_data_list = GenObjectTableDataList(*object_location_set);
  Status status =
      gcs_table_storage_->ObjectTable().Put(object_id, *object_table_data_list, on_done);
  if (!status.ok()) {
    on_done(status);
  }
}

void GcsObjectManager::HandleRemoveObjectLocation(
    const rpc::RemoveObjectLocationRequest &request,
    rpc::RemoveObjectLocationReply *reply, rpc::SendReplyCallback send_reply_callback) {
  ObjectID object_id = ObjectID::FromBinary(request.object_id());
  ClientID node_id = ClientID::FromBinary(request.node_id());
  RAY_LOG(DEBUG) << "Removing object location, job id = " << object_id.TaskId().JobId()
                 << ", object id = " << object_id << ", node id = " << node_id;
  RemoveObjectLocationInCache(object_id, node_id);

  auto on_done = [this, object_id, node_id, reply,
                  send_reply_callback](const Status &status) {
    if (status.ok()) {
      RAY_CHECK_OK(gcs_pub_sub_->Publish(
          OBJECT_CHANNEL, object_id.Hex(),
          gcs::CreateObjectLocationChange(node_id, false)->SerializeAsString(), nullptr));
      RAY_LOG(DEBUG) << "Finished removing object location, job id = "
                     << object_id.TaskId().JobId() << ", object id = " << object_id
                     << ", node id = " << node_id;
    } else {
      RAY_LOG(ERROR) << "Failed to remove object location: " << status.ToString()
                     << ", job id = " << object_id.TaskId().JobId()
                     << ", object id = " << object_id << ", node id = " << node_id;
    }
    // We should only reply after the update is written to storage.
    // So, if GCS server crashes before writing storage, GCS client will retry this
    // request.
    GCS_RPC_SEND_REPLY(send_reply_callback, reply, status);
  };

  absl::MutexLock lock(&mutex_);
  auto object_location_set =
      GetObjectLocationSet(object_id, /* create_if_not_exist */ false);
  Status status;
  if (object_location_set != nullptr) {
    auto object_table_data_list = GenObjectTableDataList(*object_location_set);
    status = gcs_table_storage_->ObjectTable().Put(object_id, *object_table_data_list,
                                                   on_done);
  } else {
    status = gcs_table_storage_->ObjectTable().Delete(object_id, on_done);
  }

  if (!status.ok()) {
    on_done(status);
  }
}

void GcsObjectManager::AddObjectsLocation(
    const ClientID &node_id, const absl::flat_hash_set<ObjectID> &object_ids) {
  // TODO(micafan) Optimize the lock when necessary.
  // Maybe use read/write lock. Or reduce the granularity of the lock.
  absl::MutexLock lock(&mutex_);

  auto *objects_on_node = GetObjectSetByNode(node_id, /* create_if_not_exist */ true);
  objects_on_node->insert(object_ids.begin(), object_ids.end());

  for (const auto &object_id : object_ids) {
    auto *object_locations =
        GetObjectLocationSet(object_id, /* create_if_not_exist */ true);
    object_locations->emplace(node_id);
  }
}

void GcsObjectManager::AddObjectLocationInCache(const ObjectID &object_id,
                                                const ClientID &node_id) {
  absl::MutexLock lock(&mutex_);

  auto *objects_on_node = GetObjectSetByNode(node_id, /* create_if_not_exist */ true);
  objects_on_node->emplace(object_id);

  auto *object_locations =
      GetObjectLocationSet(object_id, /* create_if_not_exist */ true);
  object_locations->emplace(node_id);
}

absl::flat_hash_set<ClientID> GcsObjectManager::GetObjectLocations(
    const ObjectID &object_id) {
  absl::MutexLock lock(&mutex_);

  auto *object_locations = GetObjectLocationSet(object_id);
  if (object_locations) {
    return *object_locations;
  }
  return absl::flat_hash_set<ClientID>{};
}

void GcsObjectManager::OnNodeRemoved(const ClientID &node_id) {
  absl::MutexLock lock(&mutex_);

  ObjectSet objects_on_node;
  auto it = node_to_objects_.find(node_id);
  if (it != node_to_objects_.end()) {
    objects_on_node.swap(it->second);
    node_to_objects_.erase(it);
  }

  if (objects_on_node.empty()) {
    return;
  }

  for (const auto &object_id : objects_on_node) {
    auto *object_locations = GetObjectLocationSet(object_id);
    if (object_locations) {
      object_locations->erase(node_id);
      if (object_locations->empty()) {
        object_to_locations_.erase(object_id);
      }
    }
  }
}

void GcsObjectManager::RemoveObjectLocationInCache(const ObjectID &object_id,
                                                   const ClientID &node_id) {
  absl::MutexLock lock(&mutex_);

  auto *object_locations = GetObjectLocationSet(object_id);
  if (object_locations) {
    object_locations->erase(node_id);
    if (object_locations->empty()) {
      object_to_locations_.erase(object_id);
    }
  }

  auto *objects_on_node = GetObjectSetByNode(node_id);
  if (objects_on_node) {
    objects_on_node->erase(object_id);
    if (objects_on_node->empty()) {
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

GcsObjectManager::ObjectSet *GcsObjectManager::GetObjectSetByNode(
    const ClientID &node_id, bool create_if_not_exist) {
  ObjectSet *objects_on_node = nullptr;

  auto it = node_to_objects_.find(node_id);
  if (it != node_to_objects_.end()) {
    objects_on_node = &it->second;
  } else if (create_if_not_exist) {
    auto ret = node_to_objects_.emplace(std::make_pair(node_id, ObjectSet{}));
    RAY_CHECK(ret.second);
    objects_on_node = &(ret.first->second);
  }
  return objects_on_node;
}

std::shared_ptr<ObjectTableDataList> GcsObjectManager::GenObjectTableDataList(
    const GcsObjectManager::LocationSet &location_set) const {
  auto object_table_data_list = std::make_shared<ObjectTableDataList>();
  for (auto &node_id : location_set) {
    object_table_data_list->add_items()->set_manager(node_id.Binary());
  }
  return object_table_data_list;
}

void GcsObjectManager::LoadInitialData(const EmptyCallback &done) {
  RAY_LOG(INFO) << "Loading initial data.";
  auto callback = [this, done](
                      const std::unordered_map<ObjectID, ObjectTableDataList> &result) {
    absl::flat_hash_map<ClientID, ObjectSet> node_to_objects;
    for (auto &item : result) {
      auto object_list = item.second;
      for (int index = 0; index < object_list.items_size(); ++index) {
        node_to_objects[ClientID::FromBinary(object_list.items(index).manager())].insert(
            item.first);
      }
    }

    for (auto &item : node_to_objects) {
      AddObjectsLocation(item.first, item.second);
    }
    RAY_LOG(INFO) << "Finished loading initial data.";
    done();
  };
  RAY_CHECK_OK(gcs_table_storage_->ObjectTable().GetAll(callback));
}

}  // namespace gcs

}  // namespace ray
