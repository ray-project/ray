#ifndef RAY_STREAMING_RECEIVER_H
#define RAY_STREAMING_RECEIVER_H

#include "queue/transport.h"

namespace ray {
namespace streaming {

class DirectCallReceiver {
 public:
  virtual void OnMessage(std::shared_ptr<LocalMemoryBuffer> buffer) = 0;
  virtual std::shared_ptr<LocalMemoryBuffer> OnMessageSync(
      std::shared_ptr<LocalMemoryBuffer> buffer) = 0;
};

}  // namespace streaming
}  // namespace ray
#endif