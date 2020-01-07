#ifndef RAY_GCS_ENTRY_CHANGE_NOTIFICATION_H
#define RAY_GCS_ENTRY_CHANGE_NOTIFICATION_H

#include <ray/protobuf/gcs.pb.h>
#include <vector>

namespace ray {

namespace gcs {

/// \class EntryChangeNotification
/// EntryChangeNotification class is a template class which represent
/// notification of entry change from GCS.
template <typename Data>
class EntryChangeNotification {
 public:
  EntryChangeNotification(rpc::GcsChangeMode change_mode, Data data)
      : change_mode_(change_mode), data_(std::move(data)) {}

  EntryChangeNotification(EntryChangeNotification &&other) {
    change_mode_ = other.change_mode_;
    data_ = std::move(other.data_);
  }

  EntryChangeNotification &operator=(EntryChangeNotification &&other) {
    change_mode_ = other.change_mode_;
    data_ = std::move(other.data_);
  }

  /// Whether the entry data is removed from GCS.
  bool IsRemoved() const { return change_mode_ == rpc::GcsChangeMode::REMOVE; }

  /// Whether the entry data is added to GCS.
  bool IsAdded() const { return change_mode_ == rpc::GcsChangeMode::APPEND_OR_ADD; }

  /// Get change mode of this notification. For test only.
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

template <typename Data>
using ArrayNotification = EntryChangeNotification<std::vector<Data>>;

typedef ArrayNotification<rpc::ObjectTableData> ObjectChangeNotification;

template <typename key, typename Value>
using MapNotification =
    EntryChangeNotification<std::unordered_map<key, std::shared_ptr<Value>>>;

typedef MapNotification<std::string, rpc::ResourceTableData> ResourceChangeNotification;

}  // namespace gcs

}  // namespace ray

#endif  // RAY_GCS_ENTRY_CHANGE_NOTIFICATION_H
