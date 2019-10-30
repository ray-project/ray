#include "streaming_queue_client.h"

namespace ray {
namespace streaming {

void QueueClient::OnMessage(std::shared_ptr<LocalMemoryBuffer> buffer) {
  manager_->DispatchMessage(buffer);
}

std::shared_ptr<LocalMemoryBuffer> QueueClient::OnMessageSync(
    std::shared_ptr<LocalMemoryBuffer> buffer) {
  return manager_->DispatchMessageSync(buffer);
}

}  // namespace streaming
}  // namespace ray
