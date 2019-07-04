#ifndef RAY_GCS_GCS_CLIENT_H
#define RAY_GCS_GCS_CLIENT_H

#include <boost/asio.hpp>
#include <memory>
#include <string>
#include <vector>
#include "ray/common/status.h"
#include "ray/gcs/actor_state_accessor.h"
#include "ray/gcs/client_def.h"
#include "ray/util/logging.h"

namespace ray {

namespace gcs {

/// \class GcsClientInterface
/// Interface layer of GCS client. To read and write from the GCS,
/// Connect() must be called and return Status::OK.
/// Before exit, Disconnect() must be called.
class GcsClientInterface : public std::enable_shared_from_this<GcsClientInterface> {
 public:
  virtual ~GcsClientInterface() { RAY_CHECK(!is_connected_); }

  /// Connect to GCS Service. Non-thread safe.
  /// Call this function before calling other functions.
  ///
  /// \return Status
  virtual Status Connect(boost::asio::io_service &io_service) = 0;

  /// Disconnect with GCS Service. Non-thread safe.
  virtual void Disconnect() = 0;

  /// Get client id.
  ///
  /// \return ClientID
  const ClientID &GetClientID() const { return info_.GetClientID(); }

  /// This function is thread safe.
  virtual ActorStateAccessor &Actors() {
    RAY_DCHECK(actor_accessor_ != nullptr);
    return *actor_accessor_;
  }

 protected:
  /// Constructor of GcsClientInterface.
  ///
  /// \param option Options for client.
  /// \param info Information of this client, such as client type, client id and so on.
  GcsClientInterface(const ClientOption &option, const ClientInfo &info)
      : option_(option), info_(info) {}

  ClientOption option_;
  ClientInfo info_;

  // Is client already successfully executed Connect()
  bool is_connected_{false};

  std::unique_ptr<ActorStateAccessor> actor_accessor_;
};

}  // namespace gcs

}  // namespace ray

#endif  // RAY_GCS_GCS_CLIENT_H
