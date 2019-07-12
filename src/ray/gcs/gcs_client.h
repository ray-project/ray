#ifndef RAY_GCS_GCS_CLIENT_H
#define RAY_GCS_GCS_CLIENT_H

#include <boost/asio.hpp>
#include <boost/optional/optional.hpp>
#include <memory>
#include <string>
#include <vector>
#include "ray/common/status.h"
#include "ray/gcs/actor_state_accessor.h"
#include "ray/gcs/client_def.h"
#include "ray/gcs/gcs_client_impl.h"
#include "ray/gcs/node_state_accessor.h"
#include "ray/gcs/tables.h"
#include "ray/gcs/task_state_accessor.h"
#include "ray/util/logging.h"

namespace ray {

namespace gcs {

/// \class GcsClient
/// Interface layer of GCS client. To read and write from the GCS,
/// Connect() must be called and return Status::OK.
/// Before exit, Disconnect() must be called.
class GcsClient {
 public:
  /// Constructor of GcsClient.
  ///
  /// \param option Options for client.
  /// \param info Information of this client, such as client type, client id and so on.
  /// \param io_service The event loop that the client attached to.
  GcsClient(ClientOption option, ClientInfo info, boost::asio::io_service &io_service);

  /// Constructor of GcsClient. Use this constructor, GcsClient will create a new event
  /// loop inside.
  ///
  /// \param option Options for client.
  /// \param info Information of this client, such as client type, client id and so on.
  GcsClient(ClientOption option, ClientInfo info);

  ~GcsClient() {}

  /// Connect to GCS Service. Non-thread safe.
  ///
  /// \return Status
  Status Connect();

  /// Disconnect with GCS Service. Non-thread safe.
  void Disconnect();

  /// This function is thread safe.
  NodeStateAccessor &Nodes() {
    RAY_CHECK(node_accessor_ != nullptr);
    return *node_accessor_;
  }

  /// This function is thread safe.
  ActorStateAccessor &Actors() {
    RAY_CHECK(actor_accessor_ != nullptr);
    return *actor_accessor_;
  }

  /// This function is thread safe.
  TaskStateAccessor &Tasks() {
    RAY_CHECK(task_accessor_ != nullptr);
    return *task_accessor_;
  }

  const ClientID &GetLocalClientID() const;
  
 private:
  std::unique_ptr<GcsClientImpl> client_impl_;

  std::unique_ptr<NodeStateAccessor> node_accessor_;
  std::unique_ptr<ActorStateAccessor> actor_accessor_;
  std::unique_ptr<TaskStateAccessor> task_accessor_;
};

}  // namespace gcs

}  // namespace ray

#endif  // RAY_GCS_GCS_CLIENT_H
