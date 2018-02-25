#ifndef RAY_MOCK_GCS_CLIENT_H
#define RAY_MOCK_GCS_CLIENT_H

#include <cstdint>
#include <vector>
#include <map>
#include <unordered_map>
#include <unordered_set>

#include <boost/asio.hpp>
#include <boost/asio/error.hpp>
#include <boost/bind.hpp>

#include "ray/id.h"
#include "ray/status.h"

namespace ray {

class ObjectTable {
 public:
  ray::Status Add(const ObjectID &object_id, const ClientID &client_id);
  ray::Status Remove(const ObjectID &object_id, const ClientID &client_id);
  const std::unordered_set<ClientID, UniqueIDHasher> &GetObjectClientIDs(const ObjectID &object_id);
 private:
  std::unordered_set<ClientID, UniqueIDHasher> empty_set_;
  std::unordered_map<ObjectID,
                     std::unordered_set<ClientID, UniqueIDHasher>,
                     UniqueIDHasher> client_lookup;
};

class ClientInformation {
 public:
  ClientInformation(const ClientID &client_id, const std::string &ip, int port):
  client_id_(client_id), ip_(ip), port_(port) {}
  const ClientID &GetClientId() const {
    return client_id_;
  }
  const std::string GetIp() const {
    return ip_;
  }
  int GetPort() const {
    return port_;
  }
 private:
  ClientID client_id_;
  std::string ip_;
  int port_;
};

class ClientTable {
 public:
  typedef std::unordered_map<ClientID,
                             ClientInformation,
                             UniqueIDHasher> info_type;
  std::vector<ClientID> GetClientIds();
  const ClientInformation &GetClientInformation(const ClientID &client_id);
  ray::Status Add(const ClientID &client_id,
                  const std::string &ip,
                  int port);
  ray::Status Remove(const ClientID &client_id);
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
  ray::Status Register(const std::string &ip, int port);
  ObjectTable &object_table();
  ClientTable &client_table();
 private:
  std::unique_ptr<ObjectTable> object_table_;
  std::unique_ptr<ClientTable> client_table_;
};

}

#endif //RAY_MOCK_GCS_CLIENT_H
