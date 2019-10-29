#ifndef RAY_STREAMING_LOGGING_H
#define RAY_STREAMING_LOGGING_H
#include "ray/util/logging.h"

namespace ray {
namespace streaming {
#define STREAMING_LOG RAY_LOG
#define STREAMING_CHECK RAY_CHECK
}  // namespace streaming
}  // namespace ray

#endif  // RAY_STREAMING_LOGGING_H
