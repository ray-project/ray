#include <functional>
#include <iostream>

#include "ray/raylet/mock_gcs_client.h"

namespace ray {

ray::Status ObjectTable::GetObjectClientIds(const ray::ObjectId &object_id,
                                            const ClientIdsCallback &success,
                                            const FailCallback &fail) {
  RAY_LOG(DEBUG) << "GetObjectClientIds " << object_id;
  if (client_lookup.count(object_id) > 0) {
    if (!client_lookup[object_id].empty()) {
      std::vector<ClientId> v;
      for (auto client_id : client_lookup[object_id]) {
        v.push_back(client_id);
      }
      success(std::move(v));
      return Status::OK();
    } else {
      fail(Status::KeyError("ObjectId has no clients."));
      return Status::OK();
    }
  } else {
    fail(Status::KeyError("ObjectId doesn't exist."));
    return Status::OK();
  }
}

ray::Status ObjectTable::Add(const ObjectId &object_id, const ClientId &client_id,
                             const DoneCallback &done_callback) {
  if (client_lookup.count(object_id) == 0) {
    RAY_LOG(DEBUG) << "Add ObjectId set " << object_id;
    client_lookup[object_id] = std::unordered_set<ClientId>();
  } else if (client_lookup[object_id].count(client_id) != 0) {
    return ray::Status::KeyError("ClientId already exists.");
  }
  RAY_LOG(DEBUG) << "Insert ClientId " << client_id;
  client_lookup[object_id].insert(client_id);
  done_callback();
  return ray::Status::OK();
}

ray::Status ObjectTable::Remove(const ObjectId &object_id, const ClientId &client_id,
                                const DoneCallback &done_callback) {
  if (client_lookup.count(object_id) == 0) {
    return ray::Status::KeyError("ObjectId doesn't exist.");
  } else if (client_lookup[object_id].count(client_id) == 0) {
    return ray::Status::KeyError("ClientId doesn't exist.");
  }
  client_lookup[object_id].erase(client_id);
  done_callback();
  return ray::Status::OK();
}

ray::Status ClientTable::GetClientIds(ClientIdsCallback callback) {
  std::vector<ClientId> keys;
  keys.reserve(info_lookup.size());
  for (auto kv : info_lookup) {
    keys.push_back(kv.first);
  }
  callback(keys);
  return Status::OK();
}

void ClientTable::GetClientInformationSet(const std::vector<ClientId> &client_ids,
                                          ManyInfoCallback callback,
                                          FailCallback failcb) {
  std::vector<ClientInformation> info_vec;
  for (const auto &client_id : client_ids) {
    if (info_lookup.count(client_id) != 0) {
      info_vec.push_back(info_lookup.at(client_id));
    }
  }
  if (info_vec.empty()) {
    failcb(Status::KeyError("ClientId not found."));
  } else {
    callback(info_vec);
  }
}

void ClientTable::GetClientInformation(const ClientId &client_id,
                                       SingleInfoCallback callback, FailCallback failcb) {
  if (info_lookup.count(client_id) == 0) {
    failcb(ray::Status::KeyError("CleintID not found."));
  } else {
    callback(info_lookup.at(client_id));
  }
}

ray::Status ClientTable::Add(const ClientId &client_id, const std::string &ip,
                             uint16_t port, DoneCallback done_callback) {
  if (info_lookup.count(client_id) != 0) {
    return ray::Status::KeyError("ClientId already exists.");
  }
  info_lookup.emplace(client_id, ClientInformation(client_id, ip, port));
  done_callback();
  return ray::Status::OK();
}

ray::Status ClientTable::Remove(const ClientId &client_id, DoneCallback done_callback) {
  if (info_lookup.count(client_id) == 0) {
    return ray::Status::KeyError("ClientId doesn't exist.");
  }
  info_lookup.erase(client_id);
  done_callback();
  return ray::Status::OK();
}

ClientId GcsClient::Register(const std::string &ip, uint16_t port) {
  ClientId client_id = ClientId().from_random();
  // TODO: handle client registration failure.
  ray::Status status = client_table().Add(std::move(client_id), ip, port, []() {});
  return client_id;
}

ObjectTable &GcsClient::object_table() { return *object_table_; }

ClientTable &GcsClient::client_table() { return *client_table_; }

}  // namespace ray
