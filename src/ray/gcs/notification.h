#ifndef RAY_GCS_NOTIFICATION_DEF_H
#define RAY_GCS_NOTIFICATION_DEF_H

#include <ray/protobuf/gcs.pb.h>
#include <vector>

namespace ray {

namespace gcs {

template <typename Data>
class Notification {
 public:
  Notification(rpc::GcsChangeMode change_mode, Data data)
      : change_mode_(change_mode), data_(std::move(data)) {}

  Notification(Notification &&other) {
    this.change_mode_ = other.change_mode_;
    this.data_ = std::move(other.data_);
  }

  Notification &operator=(Notification &&other) {
    this.change_mode_ = other.change_mode_;
    this.data_ = std::move(other.data_);
  }

  rpc::GcsChangeMode GetGcsChangeMode() { return change_mode_; }

  const Data &GetData() { return data_; }

 private:
  rpc::GcsChangeMode change_mode_;
  Data data_;
};

typedef Notification<std::vector<rpc::ObjectTableData>> ObjectNotification;

}  // namespace gcs

}  // namespace ray

#endif  // RAY_GCS_NOTIFICATION_DEF_H
