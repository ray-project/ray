#ifndef _STREAMING_QUEUE_CLIENT_H_
#define _STREAMING_QUEUE_CLIENT_H_
#include "queue_handler.h"
#include "transport.h"

namespace ray {
namespace streaming {

/// The interface of the streaming queue for DataReader.
/// A ReaderClient should be created before DataReader created in Cython/Jni, and hold by
/// Jobworker. When DataReader receive a buffer from upstream DataWriter (DataReader's
/// raycall function is called), it calls `OnReaderMessage` to pass the buffer to its own
/// downstream queue, or `OnReaderMessageSync` to wait for handle result.
class ReaderClient {
 public:
  /// Construct a ReaderClient object.
  /// \param[in] core_worker CoreWorker C++ pointer of current actor
  /// \param[in] async_func DataReader's raycall function descriptor to be called by
  /// DataWriter, asynchronous semantics \param[in] sync_func DataReader's raycall
  /// function descriptor to be called by DataWriter, synchronous semantics
  ReaderClient(CoreWorker *core_worker, RayFunction &async_func, RayFunction &sync_func)
      : core_worker_(core_worker) {
    DownstreamQueueMessageHandler::peer_async_function_ = async_func;
    DownstreamQueueMessageHandler::peer_sync_function_ = sync_func;
    downstream_handler_ = ray::streaming::DownstreamQueueMessageHandler::CreateService(
        core_worker_, core_worker_->GetWorkerContext().GetCurrentActorID());
  }

  /// Post buffer to downstream queue service, asynchronously.
  void OnReaderMessage(std::shared_ptr<LocalMemoryBuffer> buffer);
  /// Post buffer to downstream queue service, synchronously.
  /// \return handle result.
  std::shared_ptr<LocalMemoryBuffer> OnReaderMessageSync(
      std::shared_ptr<LocalMemoryBuffer> buffer);

 private:
  CoreWorker *core_worker_;
  std::shared_ptr<DownstreamQueueMessageHandler> downstream_handler_;
};

/// Interface of streaming queue for DataWriter. Similar to ReaderClient.
class WriterClient {
 public:
  WriterClient(CoreWorker *core_worker, RayFunction &async_func, RayFunction &sync_func)
      : core_worker_(core_worker) {
    UpstreamQueueMessageHandler::peer_async_function_ = async_func;
    UpstreamQueueMessageHandler::peer_sync_function_ = sync_func;
    upstream_handler_ = ray::streaming::UpstreamQueueMessageHandler::CreateService(
        core_worker, core_worker_->GetWorkerContext().GetCurrentActorID());
  }

  void OnWriterMessage(std::shared_ptr<LocalMemoryBuffer> buffer);
  std::shared_ptr<LocalMemoryBuffer> OnWriterMessageSync(
      std::shared_ptr<LocalMemoryBuffer> buffer);

 private:
  CoreWorker *core_worker_;
  std::shared_ptr<UpstreamQueueMessageHandler> upstream_handler_;
};
}  // namespace streaming
}  // namespace ray
#endif