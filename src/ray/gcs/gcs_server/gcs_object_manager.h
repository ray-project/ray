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

#pragma once

#include "ray/gcs/gcs_server/gcs_node_manager.h"
#include "ray/gcs/gcs_server/gcs_table_storage.h"
#include "ray/gcs/pubsub/gcs_pub_sub.h"
#include "ray/gcs/redis_gcs_client.h"

namespace ray {

namespace gcs {

class GcsObjectManager : public rpc::ObjectInfoHandler {
 public:
  explicit GcsObjectManager(std::shared_ptr<gcs::GcsTableStorage> gcs_table_storage,
                            std::shared_ptr<gcs::GcsPubSub> &gcs_pub_sub,
                            gcs::GcsNodeManager &gcs_node_manager)
      : gcs_table_storage_(std::move(gcs_table_storage)), gcs_pub_sub_(gcs_pub_sub) {
    gcs_node_manager.AddNodeRemovedListener(
        [this](const std::shared_ptr<rpc::GcsNodeInfo> &node) {
          // All of the related actors should be reconstructed when a node is removed from
          // the GCS.
          OnNodeRemoved(ClientID::FromBinary(node->node_id()));
        });
  }

  void HandleGetObjectLocations(const rpc::GetObjectLocationsRequest &request,
                                rpc::GetObjectLocationsReply *reply,
                                rpc::SendReplyCallback send_reply_callback) override;

  void HandleGetAllObjectLocations(const rpc::GetAllObjectLocationsRequest &request,
                                   rpc::GetAllObjectLocationsReply *reply,
                                   rpc::SendReplyCallback send_reply_callback) override;

  void HandleAddObjectLocation(const rpc::AddObjectLocationRequest &request,
                               rpc::AddObjectLocationReply *reply,
                               rpc::SendReplyCallback send_reply_callback) override;

  void HandleRemoveObjectLocation(const rpc::RemoveObjectLocationRequest &request,
                                  rpc::RemoveObjectLocationReply *reply,
                                  rpc::SendReplyCallback send_reply_callback) override;

  /// Load initial data from gcs storage to memory cache asynchronously.
  /// This should be called when GCS server restarts after a failure.
  ///
  /// \param done Callback that will be called when load is complete.
  void LoadInitialData(const EmptyCallback &done);

 protected:
  typedef absl::flat_hash_set<ClientID> LocationSet;

  /// Add a location of objects.
  /// If the GCS server restarts, this function is used to reload data from storage.
  ///
  /// \param node_id The object location that will be added.
  /// \param object_ids The ids of objects which location will be added.
  void AddObjectsLocation(const ClientID &node_id,
                          const absl::flat_hash_set<ObjectID> &object_ids)
      LOCKS_EXCLUDED(mutex_);

  /// Add a new location for the given object in local cache.
  ///
  /// \param object_id The id of object.
  /// \param node_id The node id of the new location.
  void AddObjectLocationInCache(const ObjectID &object_id, const ClientID &node_id)
      LOCKS_EXCLUDED(mutex_);

  /// Get all locations of the given object.
  ///
  /// \param object_id The id of object to lookup.
  /// \return Object locations.
  LocationSet GetObjectLocations(const ObjectID &object_id) LOCKS_EXCLUDED(mutex_);

  /// Handler if a node is removed.
  ///
  /// \param node_id The node that will be removed.
  void OnNodeRemoved(const ClientID &node_id) LOCKS_EXCLUDED(mutex_);

  /// Remove object's location.
  ///
  /// \param object_id The id of the object which location will be removed.
  /// \param node_id The location that will be removed.
  void RemoveObjectLocationInCache(const ObjectID &object_id, const ClientID &node_id)
      LOCKS_EXCLUDED(mutex_);

 private:
  typedef absl::flat_hash_set<ObjectID> ObjectSet;

  std::shared_ptr<ObjectTableDataList> GenObjectTableDataList(
      const GcsObjectManager::LocationSet &location_set) const;

  /// Get object locations by object id from map.
  /// Will create it if not exist and the flag create_if_not_exist is set to true.
  ///
  /// \param object_id The id of object to lookup.
  /// \param create_if_not_exist Whether to create a new one if not exist.
  /// \return LocationSet *
  GcsObjectManager::LocationSet *GetObjectLocationSet(const ObjectID &object_id,
                                                      bool create_if_not_exist = false)
      EXCLUSIVE_LOCKS_REQUIRED(mutex_);

  /// Get objects by node id from map.
  /// Will create it if not exist and the flag create_if_not_exist is set to true.
  ///
  /// \param node_id The id of node to lookup.
  /// \param create_if_not_exist Whether to create a new one if not exist.
  /// \return ObjectSet *
  GcsObjectManager::ObjectSet *GetObjectSetByNode(const ClientID &node_id,
                                                  bool create_if_not_exist = false)
      EXCLUSIVE_LOCKS_REQUIRED(mutex_);

  mutable absl::Mutex mutex_;

  /// Mapping from object id to object locations.
  /// This is the local cache of objects' locations in the storage.
  absl::flat_hash_map<ObjectID, LocationSet> object_to_locations_ GUARDED_BY(mutex_);

  /// Mapping from node id to objects that held by the node.
  /// This is the local cache of nodes' objects in the storage.
  absl::flat_hash_map<ClientID, ObjectSet> node_to_objects_ GUARDED_BY(mutex_);

  std::shared_ptr<gcs::GcsTableStorage> gcs_table_storage_;
  std::shared_ptr<gcs::GcsPubSub> gcs_pub_sub_;
};

}  // namespace gcs

}  // namespace ray
