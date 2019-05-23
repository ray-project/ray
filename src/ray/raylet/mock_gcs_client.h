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

#include "ray/id.h"
#include "ray/status.h"

namespace ray {

class ObjectTable {
 public:
  using DoneCallback = std::function<void()>;
  using ClientIdsCallback = std::function<void(const std::vector<ray::ClientId> &)>;
  using FailCallback = std::function<void(const ray::Status &)>;
  ray::Status Add(const ObjectId &object_id, const ClientId &client_id,
                  const DoneCallback &done);
  ray::Status Remove(const ObjectId &object_id, const ClientId &client_id,
                     const DoneCallback &done);
  ray::Status GetObjectClientIds(const ObjectId &object_id, const ClientIdsCallback &,
                                 const FailCallback &);

 private:
  std::vector<ClientId> empty_set_;
  std::unordered_map<ObjectId, std::unordered_set<ClientId>> client_lookup;
};

class ClientInformation {
 public:
  ClientInformation(const ClientId &client_id, const std::string &ip_address,
                    uint16_t port)
      : client_id_(client_id), ip_address_(ip_address), port_(port) {}
  const ClientId &GetClientId() const { return client_id_; }
  const std::string &GetIp() const { return ip_address_; }
  const uint16_t &GetPort() const { return port_; }

 private:
  ClientId client_id_;
  std::string ip_address_;
  uint16_t port_;
};

class ClientTable {
 public:
  typedef std::unordered_map<ClientId, ClientInformation> info_type;

  using ClientIdsCallback = std::function<void(std::vector<ray::ClientId>)>;
  using SingleInfoCallback = std::function<void(ClientInformation info)>;
  using ManyInfoCallback = std::function<void(std::vector<ClientInformation> info_vec)>;
  using DoneCallback = std::function<void()>;
  using FailCallback = std::function<void(ray::Status)>;

  ray::Status GetClientIds(ClientIdsCallback cb);
  void GetClientInformationSet(const std::vector<ClientId> &client_ids,
                               ManyInfoCallback cb, FailCallback failcb);
  void GetClientInformation(const ClientId &client_id, SingleInfoCallback callback,
                            FailCallback failcb);
  ray::Status Add(const ClientId &client_id, const std::string &ip, uint16_t port,
                  DoneCallback cb);
  ray::Status Remove(const ClientId &client_id, DoneCallback done);

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
  ClientId Register(const std::string &ip, uint16_t port);
  ObjectTable &object_table();
  ClientTable &client_table();

 private:
  std::unique_ptr<ObjectTable> object_table_;
  std::unique_ptr<ClientTable> client_table_;
};
}  // namespace ray

#endif  // RAY_RAYLET_MOCK_GCS_CLIENT_H
