#ifndef _STREAMING_QUEUE_CLIENT_H_
#define _STREAMING_QUEUE_CLIENT_H_
#include "queue_manager.h"
#include "transport.h"

namespace ray {
namespace streaming {

class MessageHandler {
  virtual void OnMessage(std::shared_ptr<LocalMemoryBuffer> buffer) = 0;
};

class QueueClient {
 public:
  QueueClient(std::shared_ptr<QueueManager> manager) : manager_(manager) {}
  void OnMessage(std::shared_ptr<LocalMemoryBuffer> buffer);
  std::shared_ptr<LocalMemoryBuffer> OnMessageSync(
      std::shared_ptr<LocalMemoryBuffer> buffer);

 private:
  std::shared_ptr<QueueManager> manager_;
};

}  // namespace streaming
}  // namespace ray
#endif
