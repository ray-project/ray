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

#ifndef RAY_GCS_OBJECT_MANAGER_H
#define RAY_GCS_OBJECT_MANAGER_H

#include "gcs_node_manager.h"
#include "ray/gcs/pubsub/gcs_pub_sub.h"
#include "ray/gcs/redis_gcs_client.h"

namespace ray {

namespace gcs {

class GcsObjectManager : public rpc::ObjectInfoHandler {
 public:
  explicit GcsObjectManager(gcs::RedisGcsClient &gcs_client,
                            std::shared_ptr<gcs::GcsPubSub> &gcs_pub_sub,
                            gcs::GcsNodeManager &gcs_node_manager)
      : gcs_pub_sub_(gcs_pub_sub) {
    gcs_node_manager.AddNodeRemovedListener(
        [this](const std::shared_ptr<rpc::GcsNodeInfo> &node) {
          // All of the related actors should be reconstructed when a node is removed from
          // the GCS.
          RemoveNode(ClientID::FromBinary(node->node_id()));
        });
  }

  void HandleGetObjectLocations(const rpc::GetObjectLocationsRequest &request,
                                rpc::GetObjectLocationsReply *reply,
                                rpc::SendReplyCallback send_reply_callback) override;

  void HandleAddObjectLocation(const rpc::AddObjectLocationRequest &request,
                               rpc::AddObjectLocationReply *reply,
                               rpc::SendReplyCallback send_reply_callback) override;

  void HandleRemoveObjectLocation(const rpc::RemoveObjectLocationRequest &request,
                                  rpc::RemoveObjectLocationReply *reply,
                                  rpc::SendReplyCallback send_reply_callback) override;

 protected:
  /// Add a location of objects.
  ///
  /// \param node_id The object location that will be added.
  /// \param object_ids The ids of objects which location will be added.
  void AddObjectsLocation(const ClientID &node_id,
                          const absl::flat_hash_set<ObjectID> &object_ids)
      LOCKS_EXCLUDED(mutex_);

  /// Add a location of an object.
  ///
  /// \param object_id The id of object which location will be added.
  /// \param node_id The object location that will be added.
  void AddObjectLocation(const ObjectID &object_id, const ClientID &node_id)
      LOCKS_EXCLUDED(mutex_);

  /// Get object's locations.
  ///
  /// \param object_id The id of object to lookup.
  /// \return Object locations.
  absl::flat_hash_set<ClientID> GetObjectLocations(const ObjectID &object_id)
      LOCKS_EXCLUDED(mutex_);

  /// Remove a node.
  ///
  /// \param node_id The node that will be removed.
  void RemoveNode(const ClientID &node_id) LOCKS_EXCLUDED(mutex_);

  /// Remove object's location.
  ///
  /// \param object_id The id of the object which location will be removed.
  /// \param node_id The location that will be removed.
  void RemoveObjectLocation(const ObjectID &object_id, const ClientID &node_id)
      LOCKS_EXCLUDED(mutex_);

 private:
  typedef absl::flat_hash_set<ClientID> LocationSet;
  typedef absl::flat_hash_set<ObjectID> ObjectSet;

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
  GcsObjectManager::ObjectSet *GetNodeHoldObjectSet(const ClientID &node_id,
                                                    bool create_if_not_exist = false)
      EXCLUSIVE_LOCKS_REQUIRED(mutex_);

  mutable absl::Mutex mutex_;

  /// Mapping from object id to object locations.
  absl::flat_hash_map<ObjectID, LocationSet> object_to_locations_ GUARDED_BY(mutex_);

  /// Mapping from node id to objects that held by the node.
  absl::flat_hash_map<ClientID, ObjectSet> node_to_objects_ GUARDED_BY(mutex_);

  std::shared_ptr<gcs::GcsPubSub> gcs_pub_sub_;
};

}  // namespace gcs

}  // namespace ray

#endif  // RAY_GCS_OBJECT_MANAGER_H
