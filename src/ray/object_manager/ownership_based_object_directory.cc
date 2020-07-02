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

#include "ray/object_manager/ownership_based_object_directory.h"

namespace ray {

OwnershipBasedObjectDirectory::OwnershipBasedObjectDirectory(boost::asio::io_service &io_service)
    : io_service_(io_service) {}

ray::Status OwnershipBasedObjectDirectory::ReportObjectAdded(
    const ObjectID &object_id, const ClientID &client_id,
    const object_manager::protocol::ObjectInfoT &object_info) {
  return Status::OK();
}

ray::Status OwnershipBasedObjectDirectory::ReportObjectRemoved(
    const ObjectID &object_id, const ClientID &client_id,
    const object_manager::protocol::ObjectInfoT &object_info) {
  return ray::Status::OK();
};

void OwnershipBasedObjectDirectory::LookupRemoteConnectionInfo(
    RemoteConnectionInfo &connection_info) const {
}

std::vector<RemoteConnectionInfo> OwnershipBasedObjectDirectory::LookupAllRemoteConnections() const {
  std::vector<RemoteConnectionInfo> remote_connections;
  return remote_connections;
}

void OwnershipBasedObjectDirectory::HandleClientRemoved(const ClientID &client_id) {
}

ray::Status OwnershipBasedObjectDirectory::SubscribeObjectLocations(const UniqueID &callback_id,
                                                      const ObjectID &object_id,
                                                      const OnLocationsFound &callback) {
  return ray::Status::OK();
}

ray::Status OwnershipBasedObjectDirectory::UnsubscribeObjectLocations(const UniqueID &callback_id,
                                                        const ObjectID &object_id) {
  return ray::Status::OK();
}

ray::Status OwnershipBasedObjectDirectory::LookupLocations(const ObjectID &object_id,
                                             const OnLocationsFound &callback) {
  return ray::Status::OK();
}

std::string OwnershipBasedObjectDirectory::DebugString() const {
  return "";
}

}  // namespace ray
