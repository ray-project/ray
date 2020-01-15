#ifndef _STREAMING_QUEUE_TRANSPORT_H_
#define _STREAMING_QUEUE_TRANSPORT_H_

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
  /// \param[in] core_worker CoreWorker C++ pointer of current actor, which we call direct
  ///            actor call interface with.
  /// \param[in] peer_actor_id actor id of peer actor.
  Transport(CoreWorker *core_worker, const ActorID &peer_actor_id)
      : core_worker_(core_worker), peer_actor_id_(peer_actor_id) {}
  virtual ~Transport() = default;

  /// Send buffer asynchronously, peer's `function` will be called.
  /// \param[in] buffer buffer to be sent.
  /// \param[in] function the function descriptor of peer's function.
  virtual void Send(std::shared_ptr<LocalMemoryBuffer> buffer, RayFunction &function);
  /// Send buffer synchronously, peer's `function` will be called, and return the peer
  /// function's return value.
  /// \param[in] buffer buffer to be sent.
  /// \param[in] function the function descriptor of peer's function.
  /// \param[in] timeout_ms max time to wait for result.
  /// \return peer function's result.
  virtual std::shared_ptr<LocalMemoryBuffer> SendForResult(
      std::shared_ptr<LocalMemoryBuffer> buffer, RayFunction &function,
      int64_t timeout_ms);
  /// Send buffer and get result with retry.
  /// return value.
  /// \param[in] buffer buffer to be sent.
  /// \param[in] function the function descriptor of peer's function.
  /// \param[in] max retry count
  /// \param[in] timeout_ms max time to wait for result.
  /// \return peer function's result.
  std::shared_ptr<LocalMemoryBuffer> SendForResultWithRetry(
      std::shared_ptr<LocalMemoryBuffer> buffer, RayFunction &function, int retry_cnt,
      int64_t timeout_ms);

 private:
  /// Send buffer internal
  /// \param[in] buffer buffer to be sent.
  /// \param[in] function the function descriptor of peer's function.
  /// \param[in] return_num return value number of the call.
  /// \param[out] return_ids return ids from SubmitActorTask.
  virtual void SendInternal(std::shared_ptr<LocalMemoryBuffer> buffer,
                            RayFunction &function, int return_num,
                            std::vector<ObjectID> &return_ids);

 private:
  CoreWorker *core_worker_;
  ActorID peer_actor_id_;
};
}  // namespace streaming
}  // namespace ray
#endif
