#include "queue/queue_client.h"

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

}  // namespace streaming
}  // namespace ray