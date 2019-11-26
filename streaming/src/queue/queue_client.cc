#include "queue_client.h"

namespace ray {
namespace streaming {

void WriterClient::OnWriterMessage(std::shared_ptr<LocalMemoryBuffer> buffer) {
  upstream_service_->DispatchMessage(buffer);
}

std::shared_ptr<LocalMemoryBuffer> WriterClient::OnWriterMessageSync(
    std::shared_ptr<LocalMemoryBuffer> buffer) {
  return upstream_service_->DispatchMessageSync(buffer);
}

void ReaderClient::OnReaderMessage(std::shared_ptr<LocalMemoryBuffer> buffer) {
  downstream_service_->DispatchMessage(buffer);
}

std::shared_ptr<LocalMemoryBuffer> ReaderClient::OnReaderMessageSync(
    std::shared_ptr<LocalMemoryBuffer> buffer) {
  return downstream_service_->DispatchMessageSync(buffer);
}

}  // namespace streaming
}  // namespace ray