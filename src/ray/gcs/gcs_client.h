#ifndef RAY_GCS_GCS_CLIENT_H
#define RAY_GCS_GCS_CLIENT_H

#include <boost/asio.hpp>
#include <string>
#include <vector>
#include "ray/common/status.h"
#include "ray/gcs/actor_state_accessor.h"
#include "ray/gcs/node_state_accessor.h"
#include "ray/gcs/tables.h"
#include "ray/gcs/task_state_accessor.h"
#include "ray/util/logging.h"

namespace ray {

namespace gcs {

class GcsClientImpl;

class ClientOption {
 public:
  std::vector<std::pair<std::string, int>> server_list_;

  std::string password_;
  CommandType command_type_ = CommandType::kChain;

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
  ClientTableData node_info_;
};

class GcsClient {
 public:
  GcsClient(ClientOption option, ClientInfo info, boost::asio::io_service &io_service);

  GcsClient(ClientOption option, ClientInfo info);

  Status Connect();

  void Disconnect();

  NodeStateAccessor &Node() {
    RAY_CHECK(node_state_accessor_ != nullptr);
    return *node_state_accessor_;
  }

  ActorStateAccessor &Actor() {
    RAY_CHECK(actor_state_accessor_ != nullptr);
    return *actor_state_accessor_;
  }

  TaskStateAccessor &Task() {
    RAY_CHECK(task_state_accessor_ != nullptr);
    return *task_state_accessor_;
  }

 private:
  std::unique_ptr<GcsClientImpl> client_impl_;

  std::unique_ptr<NodeStateAccessor> node_state_accessor_;
  std::unique_ptr<ActorStateAccessor> actor_state_accessor_;
  std::unique_ptr<TaskStateAccessor> task_state_accessor_;
};

}  // namespace gcs

}  // namespace ray

#endif  // RAY_GCS_GCS_CLIENT_H
