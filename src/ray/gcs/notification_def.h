#ifndef RAY_GCS_NOTIFICATION_DEF_H
#define RAY_GCS_NOTIFICATION_DEF_H

#include <ray/protobuf/gcs.pb.h>
#include <vector>

namespace ray {

namespace gcs {

struct ObjectNotification {
  GcsChangeMode change_mode_;
  std::vector<ray::rpc::ObjectTableData> object_data_;
};

}  // namespace gcs

}  // namespace ray

#endif  // RAY_GCS_NOTIFICATION_DEF_H
