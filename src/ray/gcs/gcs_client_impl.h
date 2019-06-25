#ifndef RAY_GCS_GCS_CLIENT_IMPL_H
#define RAY_GCS_GCS_CLIENT_IMPL_H

#include <boost/asio.hpp>
#include <memory>
#include <string>
#include <vector>
#include "ray/common/status.h"
#include "ray/gcs/client.h"
#include "ray/gcs/gcs_client.h"
#include "ray/util/logging.h"

namespace ray {

namespace gcs {

class GcsClientImpl {
 public:
  GcsClientImpl(ClientOption option, ClientInfo info,
                boost::asio::io_service *io_service = nullptr);

  virtual ~GcsClientImpl();

  virtual Status Connect();

  virtual void Disconnect();

  AsyncGcsClient &AsyncClient() {
    RAY_CHECK(async_gcs_client_ != nullptr);
    return *async_gcs_client_;
  }

  const ClientInfo &GetClientInfo() { return info_; }

 private:
  ClientOption option_;
  ClientInfo info_;

  boost::asio::io_service *io_service_{nullptr};
  std::vector<std::thread> thread_pool_;

  std::unique_ptr<AsyncGcsClient> async_gcs_client_;
};

}  // namespace gcs

}  // namespace ray

#endif  // RAY_GCS_GCS_CLIENT_IMPL_H
