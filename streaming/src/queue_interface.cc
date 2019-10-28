#include "ray/util/util.h"

#include "queue_interface.h"
#include "streaming.h"

namespace ray {

inline ray::Status ConvertStatus(const arrow::Status &status) {
  if (status.ok()) {
    return Status::OK();
  }
  return ray::Status(static_cast<ray::StatusCode>(status.code()), status.message());
}

}  // namespace ray
