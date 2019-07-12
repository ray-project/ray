#ifndef RAY_GCS_GCS_CLIENT_IMPL_H
#define RAY_GCS_GCS_CLIENT_IMPL_H

#include <boost/asio.hpp>
#include <memory>
#include <string>
#include <vector>
#include "ray/common/status.h"
#include "ray/gcs/client.h"
#include "ray/gcs/client_def.h"
#include "ray/gcs/gcs_client.h"
#include "ray/util/logging.h"

namespace ray {

namespace gcs {

/// \class GcsClientImpl
/// Implementation layer of GCS client.
class GcsClientImpl {
 public:
  GcsClientImpl(ClientOption option, ClientInfo info,
                boost::asio::io_service *io_service = nullptr);

  virtual ~GcsClientImpl();

  /// Connect to GCS Service. Non-thread safe.
  ///
  /// \return Status
  virtual Status Connect();

  /// Disconnect with GCS Service. Non-thread safe.
  virtual void Disconnect();

  AsyncGcsClient &AsyncClient() {
    RAY_CHECK(async_gcs_client_ != nullptr);
    return *async_gcs_client_;
  }

  const ClientID &GetLocalClientID() const {
    return info_.id_;
  }

 private:
  ClientOption option_;
  ClientInfo info_;

  boost::asio::io_service *io_service_{nullptr};
  std::vector<std::thread> thread_pool_;
  bool is_connected_{false};

  std::unique_ptr<AsyncGcsClient> async_gcs_client_;
};

}  // namespace gcs

}  // namespace ray

#endif  // RAY_GCS_GCS_CLIENT_IMPL_H
