#ifndef _STREAMING_QUEUE_CLIENT_H_
#define _STREAMING_QUEUE_CLIENT_H_
#include "queue_service.h"
#include "transport.h"

namespace ray {
namespace streaming {

class ReaderClient {
public:
  ReaderClient(CoreWorker *core_worker, RayFunction &async_func, RayFunction &sync_func)
  : core_worker_(core_worker) {
    DownstreamService::PEER_ASYNC_FUNCTION = async_func;
    DownstreamService::PEER_SYNC_FUNCTION = sync_func;
    downstream_service_ = ray::streaming::DownstreamService::GetService(core_worker_, core_worker_->GetWorkerContext().GetCurrentActorID());
  }

  void OnReaderMessage(std::shared_ptr<LocalMemoryBuffer> buffer);
  std::shared_ptr<LocalMemoryBuffer> OnReaderMessageSync(std::shared_ptr<LocalMemoryBuffer> buffer);

private:
  CoreWorker *core_worker_;
  std::shared_ptr<DownstreamService> downstream_service_;
};

class WriterClient {
public:
  WriterClient(CoreWorker *core_worker, RayFunction &async_func, RayFunction &sync_func) 
  : core_worker_(core_worker) {
    UpstreamService::PEER_ASYNC_FUNCTION = async_func;
    UpstreamService::PEER_SYNC_FUNCTION = sync_func;
    upstream_service_ = ray::streaming::UpstreamService::GetService(core_worker, core_worker_->GetWorkerContext().GetCurrentActorID());
  }

  void OnWriterMessage(std::shared_ptr<LocalMemoryBuffer> buffer);
  std::shared_ptr<LocalMemoryBuffer> OnWriterMessageSync(std::shared_ptr<LocalMemoryBuffer> buffer);

private:
  CoreWorker *core_worker_;
  std::shared_ptr<UpstreamService> upstream_service_;
};
}  // namespace streaming
}  // namespace ray
#endif