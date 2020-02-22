#ifndef RAY_GCS_GCS_CLIENT_H
#define RAY_GCS_GCS_CLIENT_H

#include <boost/asio.hpp>
#include <memory>
#include <string>
#include <vector>
#include "ray/common/status.h"
#include "ray/gcs/accessor.h"
#include "ray/util/logging.h"

namespace ray {

namespace gcs {

/// \class GcsClientOptions
/// GCS client's options (configuration items), such as service address, and service
/// password.
class GcsClientOptions {
 public:
  /// Constructor of GcsClientOptions.
  ///
  /// \param ip GCS service ip.
  /// \param port GCS service port.
  /// \param password GCS service password.
  /// \param is_test_client Whether this client is used for tests.
  GcsClientOptions(const std::string &ip, int port, const std::string &password,
                   bool is_test_client = false);

  // GCS server address
  std::string server_ip_;
  int server_port_;

  // Password of GCS server.
  std::string password_;

  // Whether this client is used for tests.
  bool is_test_client_{false};
};

/// \class GcsClient
/// Abstract interface of the GCS client.
///
/// To read and write from the GCS, `Connect()` must be called and return Status::OK.
/// Before exit, `Disconnect()` must be called.
class GcsClient : public std::enable_shared_from_this<GcsClient> {
 public:
  virtual ~GcsClient();

  /// Connect to GCS Service. Non-thread safe.
  /// This function must be called before calling other functions.
  ///
  /// \return Status
  virtual Status Connect(boost::asio::io_service &io_service) = 0;

  /// Disconnect with GCS Service. Non-thread safe.
  virtual void Disconnect() = 0;

  /// Return client information for debug.
  virtual std::string DebugString() const;

  /// Get the sub-interface for accessing actor information in GCS.
  /// This function is thread safe.
  ActorInfoAccessor &Actors();

  /// Get the sub-interface for accessing job information in GCS.
  /// This function is thread safe.
  JobInfoAccessor &Jobs();

  /// Get the sub-interface for accessing object information in GCS.
  /// This function is thread safe.
  ObjectInfoAccessor &Objects();

  /// Get the sub-interface for accessing node information in GCS.
  /// This function is thread safe.
  NodeInfoAccessor &Nodes();

  /// Get the sub-interface for accessing task information in GCS.
  /// This function is thread safe.
  TaskInfoAccessor &Tasks();

  /// Get the sub-interface for accessing error information in GCS.
  /// This function is thread safe.
  ErrorInfoAccessor &Errors();

  /// Get the sub-interface for accessing stats information in GCS.
  /// This function is thread safe.
  StatsInfoAccessor &Stats();

  /// Get the sub-interface for accessing worker information in GCS.
  /// This function is thread safe.
  WorkerInfoAccessor &Workers();

 protected:
  /// Constructor of GcsClient.
  ///
  /// \param options Options for client.
  GcsClient(const GcsClientOptions &options);

  GcsClientOptions options_;

  /// Whether this client is connected to GCS.
  bool is_connected_{false};

  std::unique_ptr<ActorInfoAccessor> actor_accessor_;
  std::unique_ptr<JobInfoAccessor> job_accessor_;
  std::unique_ptr<ObjectInfoAccessor> object_accessor_;
  std::unique_ptr<NodeInfoAccessor> node_accessor_;
  std::unique_ptr<TaskInfoAccessor> task_accessor_;
  std::unique_ptr<ErrorInfoAccessor> error_accessor_;
  std::unique_ptr<StatsInfoAccessor> stats_accessor_;
  std::unique_ptr<WorkerInfoAccessor> worker_accessor_;
};

}  // namespace gcs

}  // namespace ray

#endif  // RAY_GCS_GCS_CLIENT_H
