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

#ifndef RAY_RAYLET_MOCK_GCS_CLIENT_H
#define RAY_RAYLET_MOCK_GCS_CLIENT_H

#include <cstdint>
#include <functional>
#include <map>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include <boost/asio.hpp>
#include <boost/asio/error.hpp>
#include <boost/bind.hpp>

#include "ray/common/id.h"
#include "ray/common/status.h"

namespace ray {

class ObjectTable {
 public:
  using DoneCallback = std::function<void()>;
  using ClientIDsCallback = std::function<void(const std::vector<ray::ClientID> &)>;
  using FailCallback = std::function<void(const ray::Status &)>;
  ray::Status Add(const ObjectID &object_id, const ClientID &client_id,
                  const DoneCallback &done);
  ray::Status Remove(const ObjectID &object_id, const ClientID &client_id,
                     const DoneCallback &done);
  ray::Status GetObjectClientIDs(const ObjectID &object_id, const ClientIDsCallback &,
                                 const FailCallback &);

 private:
  std::vector<ClientID> empty_set_;
  std::unordered_map<ObjectID, std::unordered_set<ClientID>> client_lookup;
};

class ClientInformation {
 public:
  ClientInformation(const ClientID &client_id, const std::string &ip_address,
                    uint16_t port)
      : client_id_(client_id), ip_address_(ip_address), port_(port) {}
  const ClientID &GetClientId() const { return client_id_; }
  const std::string &GetIp() const { return ip_address_; }
  const uint16_t &GetPort() const { return port_; }

 private:
  ClientID client_id_;
  std::string ip_address_;
  uint16_t port_;
};

class ClientTable {
 public:
  typedef std::unordered_map<ClientID, ClientInformation> info_type;

  using ClientIDsCallback = std::function<void(std::vector<ray::ClientID>)>;
  using SingleInfoCallback = std::function<void(ClientInformation info)>;
  using ManyInfoCallback = std::function<void(std::vector<ClientInformation> info_vec)>;
  using DoneCallback = std::function<void()>;
  using FailCallback = std::function<void(ray::Status)>;

  ray::Status GetClientIds(ClientIDsCallback cb);
  void GetClientInformationSet(const std::vector<ClientID> &client_ids,
                               ManyInfoCallback cb, FailCallback failcb);
  void GetClientInformation(const ClientID &client_id, SingleInfoCallback callback,
                            FailCallback failcb);
  ray::Status Add(const ClientID &client_id, const std::string &ip, uint16_t port,
                  DoneCallback cb);
  ray::Status Remove(const ClientID &client_id, DoneCallback done);

 private:
  info_type info_lookup;
};

class GcsClient {
 public:
  GcsClient() {
    this->object_table_.reset(new ObjectTable());
    this->client_table_.reset(new ClientTable());
  }
  // Register the ip and port of the connecting client.
  ClientID Register(const std::string &ip, uint16_t port);
  ClientTable &client_table();

 private:
  std::unique_ptr<ObjectTable> object_table_;
  std::unique_ptr<ClientTable> client_table_;
};
}  // namespace ray

#endif  // RAY_RAYLET_MOCK_GCS_CLIENT_H
