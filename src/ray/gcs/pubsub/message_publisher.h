#ifndef RAY_GCS_PUBSUB_MESSAGE_PUBLISHER_H
#define RAY_GCS_PUBSUB_MESSAGE_PUBLISHER_H

#include <string>
#include "ray/common/status.h"
#include "ray/gcs/callback.h"
#include "ray/util/io_service_pool.h"
#include "ray/util/logging.h"

namespace ray {

namespace gcs {

class MessagePublisher {
 public:
  virtual ~MessagePublisher() {}

  Status Connect(std::shared_ptr<IOServicePool> io_service_pool) = 0;

  void Disconnect() = 0;

  virtual Status PublishMessage(const std::string &channel, const std::string &message,
                                const StatusCallback &callback) = 0;

  template <typename Message>
  virtual Status PublishMessage(const std::string &channel, const Message &message,
                                const StatusCallback &callback) = 0;

 protected:
  MessagePublisher() {}
};

}  // namespace gcs

}  // namespace ray

#endif  // RAY_GCS_PUBSUB_MESSAGE_PUBLISHER_H
