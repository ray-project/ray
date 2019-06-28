#ifndef RAY_GCS_CLIENT_DEF_H
#define RAY_GCS_CLIENT_DEF_H

#include <boost/optional/optional.hpp>
#include <string>
#include <vector>
#include "ray/gcs/tables.h"

namespace ray {

namespace gcs {

class ClientOption {
 public:
  // GCS server list: <ip, port>
  std::vector<std::pair<std::string, int>> server_list_;

  // Password of GCS server.
  std::string password_;
  // GCS command type. If CommandType::kChain, chain-replicated versions of the tables
  // might be used, if available.
  CommandType command_type_ = CommandType::kChain;

  // If it's test client.
  bool test_mode_{false};
};

class ClientInfo {
 public:
  enum class ClientType {
    kClientTypeRaylet,
    kClientTypeRayletMonitor,
    kClientTypeWorker,
  };

  ClientType type_;
  ClientID id_;
  // This field is required when the client type is raylet.
  boost::optional<ClientTableData> node_info_;
};

}  // namespace gcs

}  // namespace ray

#endif  // RAY_GCS_CLIENT_DEF_H
