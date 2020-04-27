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

#include <functional>
#include <iostream>

#include "ray/raylet/mock_gcs_client.h"

namespace ray {

ray::Status ObjectTable::GetObjectClientIDs(const ray::ObjectID &object_id,
                                            const ClientIDsCallback &success,
                                            const FailCallback &fail) {
  RAY_LOG(DEBUG) << "GetObjectClientIDs " << object_id;
  if (client_lookup.count(object_id) > 0) {
    if (!client_lookup[object_id].empty()) {
      std::vector<ClientID> v;
      for (auto client_id : client_lookup[object_id]) {
        v.push_back(client_id);
      }
      success(std::move(v));
      return Status::OK();
    } else {
      fail(Status::KeyError("ObjectID has no clients."));
      return Status::OK();
    }
  } else {
    fail(Status::KeyError("ObjectID doesn't exist."));
    return Status::OK();
  }
}

ray::Status ObjectTable::Add(const ObjectID &object_id, const ClientID &client_id,
                             const DoneCallback &done_callback) {
  if (client_lookup.count(object_id) == 0) {
    RAY_LOG(DEBUG) << "Add ObjectID set " << object_id;
    client_lookup[object_id] = std::unordered_set<ClientID>();
  } else if (client_lookup[object_id].count(client_id) != 0) {
    return ray::Status::KeyError("ClientID already exists.");
  }
  RAY_LOG(DEBUG) << "Insert ClientID " << client_id;
  client_lookup[object_id].insert(client_id);
  done_callback();
  return ray::Status::OK();
}

ray::Status ObjectTable::Remove(const ObjectID &object_id, const ClientID &client_id,
                                const DoneCallback &done_callback) {
  if (client_lookup.count(object_id) == 0) {
    return ray::Status::KeyError("ObjectID doesn't exist.");
  } else if (client_lookup[object_id].count(client_id) == 0) {
    return ray::Status::KeyError("ClientID doesn't exist.");
  }
  client_lookup[object_id].erase(client_id);
  done_callback();
  return ray::Status::OK();
}

ray::Status ClientTable::GetClientIds(ClientIDsCallback callback) {
  std::vector<ClientID> keys;
  keys.reserve(info_lookup.size());
  for (auto kv : info_lookup) {
    keys.push_back(kv.first);
  }
  callback(keys);
  return Status::OK();
}

void ClientTable::GetClientInformationSet(const std::vector<ClientID> &client_ids,
                                          ManyInfoCallback callback,
                                          FailCallback failcb) {
  std::vector<ClientInformation> info_vec;
  for (const auto &client_id : client_ids) {
    if (info_lookup.count(client_id) != 0) {
      info_vec.push_back(info_lookup.at(client_id));
    }
  }
  if (info_vec.empty()) {
    failcb(Status::KeyError("ClientID not found."));
  } else {
    callback(info_vec);
  }
}

void ClientTable::GetClientInformation(const ClientID &client_id,
                                       SingleInfoCallback callback, FailCallback failcb) {
  if (info_lookup.count(client_id) == 0) {
    failcb(ray::Status::KeyError("CleintID not found."));
  } else {
    callback(info_lookup.at(client_id));
  }
}

ray::Status ClientTable::Add(const ClientID &client_id, const std::string &ip,
                             uint16_t port, DoneCallback done_callback) {
  if (info_lookup.count(client_id) != 0) {
    return ray::Status::KeyError("ClientID already exists.");
  }
  info_lookup.emplace(client_id, ClientInformation(client_id, ip, port));
  done_callback();
  return ray::Status::OK();
}

ray::Status ClientTable::Remove(const ClientID &client_id, DoneCallback done_callback) {
  if (info_lookup.count(client_id) == 0) {
    return ray::Status::KeyError("ClientID doesn't exist.");
  }
  info_lookup.erase(client_id);
  done_callback();
  return ray::Status::OK();
}

ClientID GcsClient::Register(const std::string &ip, uint16_t port) {
  ClientID client_id = ClientID::FromRandom();
  // TODO: handle client registration failure.
  ray::Status status = client_table().Add(std::move(client_id), ip, port, []() {});
  return client_id;
}

ClientTable &GcsClient::client_table() { return *client_table_; }

}  // namespace ray
