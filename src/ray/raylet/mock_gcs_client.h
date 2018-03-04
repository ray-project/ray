#ifndef RAY_MOCK_GCS_CLIENT_H
#define RAY_MOCK_GCS_CLIENT_H

#include <cstdint>
#include <vector>
#include <map>
#include <functional>
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
  using DoneCallback = std::function<void()>;
  using ClientIDsCallback = std::function<void(const std::vector<ray::ClientID>&)>;
  using FailCallback = std::function<void(const ray::Status&)>;
  ray::Status Add(const ObjectID &object_id,
                  const ClientID &client_id,
                  const DoneCallback &done);
  ray::Status Remove(const ObjectID &object_id,
                     const ClientID &client_id,
                     const DoneCallback &done);
  ray::Status GetObjectClientIDs(const ObjectID &object_id,
                                 const ClientIDsCallback&,
                                 const FailCallback&);

 private:
  std::vector<ClientID> empty_set_;
  std::unordered_map<ObjectID,
                     std::unordered_set<ClientID, UniqueIDHasher>,
                     UniqueIDHasher> client_lookup;
};

class ClientInformation {
 public:
  ClientInformation(const ClientID &client_id, const std::string &ip, ushort port):
  client_id_(client_id), ip_(ip), port_(port) {}
  const ClientID &GetClientId() const {
    return client_id_;
  }
  const std::string &GetIp() const {
    return ip_;
  }
  const ushort &GetPort() const {
    return port_;
  }
 private:
  ClientID client_id_;
  std::string ip_;
  ushort port_;
};

class ClientTable {
 public:
  typedef std::unordered_map<ClientID,
                             ClientInformation,
                             UniqueIDHasher> info_type;

  using ClientIDsCallback = std::function<void(std::vector<ray::ClientID>)>;
  using SingleInfoCallback  = std::function<void(ClientInformation info)>;
  using ManyInfoCallback  = std::function<void(std::vector<ClientInformation> info_vec)>;
  using DoneCallback = std::function<void()>;
  using FailCallback = std::function<void(ray::Status)>;

  ray::Status GetClientIds(ClientIDsCallback cb);
  void GetClientInformationSet(const std::vector<ClientID> &client_ids,
                               ManyInfoCallback cb,
                               FailCallback failcb);
  void GetClientInformation(ClientID client_id,
                            SingleInfoCallback cb,
                            FailCallback failcb);
  ray::Status Add(const ClientID &client_id,
                  const std::string &ip,
                  ushort port,
                  DoneCallback cb);
  ray::Status Remove(const ClientID &client_id,
                     DoneCallback done);
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
  ClientID Register(const std::string &ip, ushort port);
  ObjectTable &object_table();
  ClientTable &client_table();
 private:
  std::unique_ptr<ObjectTable> object_table_;
  std::unique_ptr<ClientTable> client_table_;
};

}

#endif //RAY_MOCK_GCS_CLIENT_H
