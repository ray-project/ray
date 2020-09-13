#pragma once

#include "ray/common/id.h"
#include "ray/core_worker/core_worker.h"
#include "util/streaming_logging.h"

namespace ray {
namespace streaming {

/// Transport is the transfer endpoint to a specific actor, buffers can be sent to peer
/// through direct actor call.
class Transport {
 public:
  /// Construct a Transport object.
  /// \param[in] peer_actor_id actor id of peer actor.
  Transport(const ActorID &peer_actor_id, RayFunction &async_func, RayFunction &sync_func)
      : peer_actor_id_(peer_actor_id), async_func_(async_func), sync_func_(sync_func) {
    STREAMING_LOG(INFO) << "Transport constructor:";
    STREAMING_LOG(INFO) << "async_func lang: " << async_func_.GetLanguage();
    STREAMING_LOG(INFO) << "async_func: "
                        << async_func_.GetFunctionDescriptor()->ToString();
    STREAMING_LOG(INFO) << "sync_func lang: " << sync_func_.GetLanguage();
    STREAMING_LOG(INFO) << "sync_func: "
                        << sync_func_.GetFunctionDescriptor()->ToString();
  }

  virtual ~Transport() = default;

  /// Send buffer asynchronously, peer's `function` will be called.
  /// \param[in] buffer buffer to be sent.
  virtual void Send(std::shared_ptr<LocalMemoryBuffer> buffer);

  /// Send buffer synchronously, peer's `function` will be called, and return the peer
  /// function's return value.
  /// \param[in] buffer buffer to be sent.
  /// \param[in] timeout_ms max time to wait for result.
  /// \return peer function's result.
  virtual std::shared_ptr<LocalMemoryBuffer> SendForResult(
      std::shared_ptr<LocalMemoryBuffer> buffer, int64_t timeout_ms);

  /// Send buffer and get result with retry.
  /// return value.
  /// \param[in] buffer buffer to be sent.
  /// \param[in] max retry count
  /// \param[in] timeout_ms max time to wait for result.
  /// \return peer function's result.
  std::shared_ptr<LocalMemoryBuffer> SendForResultWithRetry(
      std::shared_ptr<LocalMemoryBuffer> buffer, int retry_cnt, int64_t timeout_ms);

 private:
  WorkerID worker_id_;
  ActorID peer_actor_id_;
  RayFunction async_func_;
  RayFunction sync_func_;
};
}  // namespace streaming
}  // namespace ray
