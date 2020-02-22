#include "queue_client.h"

namespace ray {
namespace streaming {

void WriterClient::OnWriterMessage(std::shared_ptr<LocalMemoryBuffer> buffer) {
  upstream_handler_->DispatchMessageAsync(buffer);
}

std::shared_ptr<LocalMemoryBuffer> WriterClient::OnWriterMessageSync(
    std::shared_ptr<LocalMemoryBuffer> buffer) {
  return upstream_handler_->DispatchMessageSync(buffer);
}

void ReaderClient::OnReaderMessage(std::shared_ptr<LocalMemoryBuffer> buffer) {
  downstream_handler_->DispatchMessageAsync(buffer);
}

std::shared_ptr<LocalMemoryBuffer> ReaderClient::OnReaderMessageSync(
    std::shared_ptr<LocalMemoryBuffer> buffer) {
  return downstream_handler_->DispatchMessageSync(buffer);
}

ReaderClient::ReaderClient(CoreWorker *core_worker, RayFunction &async_func,
                           RayFunction &sync_func)
    : core_worker_(core_worker) {
  DownstreamQueueMessageHandler::peer_async_function_ = async_func;
  DownstreamQueueMessageHandler::peer_sync_function_ = sync_func;
  downstream_handler_ = ray::streaming::DownstreamQueueMessageHandler::CreateService(
      core_worker_, core_worker_->GetWorkerContext().GetCurrentActorID());
}
WriterClient::WriterClient(CoreWorker *core_worker, RayFunction &async_func,
                           RayFunction &sync_func)
    : core_worker_(core_worker) {
  UpstreamQueueMessageHandler::peer_async_function_ = async_func;
  UpstreamQueueMessageHandler::peer_sync_function_ = sync_func;
  upstream_handler_ = ray::streaming::UpstreamQueueMessageHandler::CreateService(
      core_worker, core_worker_->GetWorkerContext().GetCurrentActorID());
}

}  // namespace streaming
}  // namespace ray
