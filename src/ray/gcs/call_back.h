#ifndef RAY_GCS_CALL_BACK_H
#define RAY_GCS_CALL_BACK_H

#include <vector>
#include <boost/optional/optional.hpp>
#include "ray/common/status.h"

namespace ray {

namespace gcs {

using StatusCallback = std::function<void(Status status)>;

template <typename T>
class DatumCallback {
 public:
  using SingleItem = std::function<void(Status status, boost::optional<T> datum)>;

  using MultiItem =std::function<void(Status status, std::vector<T> datums)>;
};

}  // namespace gcs

}  // namespace ray

#endif  // RAY_GCS_CALL_BACK_H
