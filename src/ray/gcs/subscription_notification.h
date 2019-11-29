#ifndef RAY_GCS_NOTIFICATION_DEF_H
#define RAY_GCS_NOTIFICATION_DEF_H

#include <ray/protobuf/gcs.pb.h>
#include <vector>

namespace ray {

namespace gcs {

/// \class SubscriptionNotification
/// SubscriptionNotification class is a template class which represent
/// notification of subscription request.
template <typename Data>
class SubscriptionNotification {
 public:
  SubscriptionNotification(rpc::GcsChangeMode change_mode, Data data)
      : change_mode_(change_mode), data_(std::move(data)) {}

  SubscriptionNotification(SubscriptionNotification &&other) {
    change_mode_ = other.change_mode_;
    data_ = std::move(other.data_);
  }

  SubscriptionNotification &operator=(SubscriptionNotification &&other) {
    change_mode_ = other.change_mode_;
    data_ = std::move(other.data_);
  }

  /// Get change mode of this notification.
  ///
  /// \return rpc::GcsChangeMode
  rpc::GcsChangeMode GetGcsChangeMode() const { return change_mode_; }

  /// Get data of this notification.
  ///
  /// \return Data
  const Data &GetData() const { return data_; }

 private:
  rpc::GcsChangeMode change_mode_;
  Data data_;
};

typedef SubscriptionNotification<std::vector<rpc::ObjectTableData>> ObjectNotification;

typedef SubscriptionNotification<std::vector<rpc::ActorTableData>> ActorNotification;

}  // namespace gcs

}  // namespace ray

#endif  // RAY_GCS_NOTIFICATION_DEF_H
