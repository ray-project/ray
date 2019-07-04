#ifndef RAY_GCS_CLIENT_DEF_H
#define RAY_GCS_CLIENT_DEF_H

#include <boost/optional/optional.hpp>
#include <string>
#include "ray/gcs/tables.h"

namespace ray {

namespace gcs {

class ClientOption {
 public:
  ClientOption(const std::string &ip, int port, const std::string &password,
               bool is_test_client = false)
      : server_ip_(ip),
        server_port_(port),
        password_(password),
        is_test_client_(is_test_client) {
#if RAY_USE_NEW_GCS
    command_type_ = CommandType::kChain;
#else
    command_type_ = CommandType::kRegular;
#endif
  }

  /// This constructor is only used for testing(test of RedisGcsClient).
  ClientOption(const std::string &ip, int port, CommandType command_type)
      : server_ip_(ip),
        server_port_(port),
        command_type_(command_type),
        is_test_client_(true) {}

  // GCS server address
  std::string server_ip_;
  int server_port_;

  // Password of GCS server.
  std::string password_;
  // GCS command type. If CommandType::kChain, chain-replicated versions of the tables
  // might be used, if available.
  CommandType command_type_ = CommandType::kChain;

  // If it's test client.
  bool is_test_client_{false};
};

class ClientInfo {
 public:
  /// Constructor for worker, raylet monitor
  ClientInfo() : id_(ClientID::FromRandom()) {}

  /// Constructor for worker, raylet monitor
  explicit ClientInfo(const ClientID &id) : id_(id) {}

  /// Constructor for raylet
  ClientInfo(const ClientTableData &client_data) : client_data_(client_data) {
    id_ = ClientID::FromBinary(client_data.client_id());
  }

  const ClientID &GetClientID() const { return id_; }

  bool IsRaylet() { return !!client_data_; }

 private:
  ClientID id_;
  // for raylet register to gcs and do heartbeat
  boost::optional<ClientTableData> client_data_;
};

}  // namespace gcs

}  // namespace ray

#endif  // RAY_GCS_CLIENT_DEF_H
