#ifndef RAY_GCS_CLIENT_DEF_H
#define RAY_GCS_CLIENT_DEF_H

#include <boost/optional/optional.hpp>
#include <string>
#include "ray/gcs/tables.h"

namespace ray {

namespace gcs {

/// \class ClientOption
/// GCS client's options(configuration items), such as service address, service password.
class ClientOption {
 public:
  /// Constructor of ClientOption.
  ///
  /// \param ip GCS service ip
  /// \param port GCS service port
  /// \param password GCS service password
  /// \param is_test_client Is test client
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

  /// This constructor is only used for testing(RedisGcsClient's test).
  ///
  /// \param ip Gcs service ip
  /// \param port Gcs service port
  /// \param command_type Command type of RedisGcsClient
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

/// \class ClientInfo
/// Information of GCS client, such as client id.
/// Only if it's raylet'client, it should contain much more information included by
/// ClientTableData:
/// node manager's address, object manager's port and so on.
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

  const ClientID &GetClientID() const {
    RAY_DCHECK(!id_.IsNil());
    return id_;
  }

  bool IsRaylet() { return client_data_.is_initialized(); }

 private:
  ClientID id_;
  // raylet will register this message to gcs(will do heartbeat after register)
  boost::optional<ClientTableData> client_data_;
};

}  // namespace gcs

}  // namespace ray

#endif  // RAY_GCS_CLIENT_DEF_H
