#ifndef RAY_GCS_GCS_CLIENT_H
#define RAY_GCS_GCS_CLIENT_H

#include <boost/asio.hpp>
#include <memory>
#include <string>
#include <vector>
#include "ray/common/status.h"
#include "ray/gcs/actor_state_accessor.h"
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

  /// This constructor is only used for testing (RedisGcsClient's test).
  ///
  /// \param ip GCS service ip
  /// \param port GCS service port
  /// \param command_type Command type of RedisGcsClient
  GcsClientOptions(const std::string &ip, int port, CommandType command_type)
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
  CommandType command_type_ = CommandType::kUnknown;

  // Whether this client is used for tests.
  bool is_test_client_{false};
};

/// \class GcsClientInterface
/// Abstract interface of the GCS client.
///
/// To read and write from the GCS, `Connect()` must be called and return Status::OK.
/// Before exit, `Disconnect()` must be called.
class GcsClientInterface : public std::enable_shared_from_this<GcsClientInterface> {
 public:
  virtual ~GcsClientInterface() {}

  /// Connect to GCS Service. Non-thread safe.
  /// This function must be called before calling other functions.
  ///
  /// \return Status
  virtual Status Connect(boost::asio::io_service &io_service) = 0;

  /// Disconnect with GCS Service. Non-thread safe.
  virtual void Disconnect() = 0;

  /// Get ActorStateAccessor for reading or writing or subscribing to
  /// actors. This function is thread safe.
  ActorStateAccessor &Actors() {
    RAY_CHECK(actor_accessor_ != nullptr);
    return *actor_accessor_;
  }

 protected:
  /// Constructor of GcsClientInterface.
  ///
  /// \param options Options for client.
  GcsClientInterface(const GcsClientOptions &options) : options_(options) {}

  GcsClientOptions options_;

  // Whether this client is connected to GCS.
  bool is_connected_{false};

  std::unique_ptr<ActorStateAccessor> actor_accessor_;
};

}  // namespace gcs

}  // namespace ray

#endif  // RAY_GCS_GCS_CLIENT_H
