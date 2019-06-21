#ifndef RAY_GCS_GCS_CLIENT_H
#define RAY_GCS_GCS_CLIENT_H

#include <boost/asio.hpp>
#include <string>
#include <vector>
#include "ray/common/status.h"
#include "ray/gcs/format/gcs_generated.h"
#include "ray/gcs/tables.h"
#include "ray/util/logging.h"

namespace ray {

namespace gcs {

class GcsClientImpl;

class ActorStateAccessor;
class NodeStateAccessor;
class TaskStateAccessor;

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
  ClientTableDataT node_info_;
};

class GcsClient {
 public:
  GcsClient(ClientOption option, ClientInfo info, boost::asio::io_service &io_service);

  GcsClient(ClientOption option, ClientInfo info);

  ~GcsClient();

  Status Connect();

  Status Disconnect();

  NodeStateAccessor *Node() {
    RAY_CHECK(node_state_accessor_ != nullptr);
    return node_state_accessor_;
  }

  ActorStateAccessor *Actor() {
    RAY_CHECK(actor_state_accessor_ != nullptr);
    return actor_state_accessor_;
  }

  TaskStateAccessor *Task() {
    RAY_CHECK(task_state_accessor_ != nullptr);
    return task_state_accessor_;
  }

 private:
  ClientOption option_;
  ClientInfo info_;

  boost::asio::io_service *io_service_{nullptr};
  std::vector<std::thread> thread_pool_;

  // GcsClientImpl
  AsyncGcsClient *client_impl_{nullptr};

  NodeStateAccessor *node_state_accessor_{nullptr};
  ActorStateAccessor *actor_state_accessor_{nullptr};
  TaskStateAccessor *task_state_accessor_{nullptr};
};

}  // namespace gcs

}  // namespace ray

#endif  // RAY_GCS_GCS_CLIENT_H
