// Copyright 2017 The Ray Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//  http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#ifndef RAY_OBJECT_STORE_NOTIFICATION_MANAGER_H
#define RAY_OBJECT_STORE_NOTIFICATION_MANAGER_H

#include <boost/asio.hpp>
#include <iostream>
#include <memory>
#include <vector>

#include "absl/synchronization/mutex.h"
#include "ray/common/id.h"
#include "ray/common/status.h"
#include "ray/object_manager/format/object_manager_generated.h"

namespace ray {

/// \class ObjectStoreNotificationManager
///
/// Encapsulates notification handling from the object store.
class ObjectStoreNotificationManager {
 public:
  ObjectStoreNotificationManager(boost::asio::io_service &io_service)
      : main_service_(&io_service), num_adds_processed_(0), num_removes_processed_(0) {}
  virtual ~ObjectStoreNotificationManager() {}
  /// Subscribe to notifications of objects added to local store.
  /// Upon subscribing, the callback will be invoked for all objects that
  /// already exist in the local store
  ///
  /// \param callback A callback expecting an ObjectID.
  void SubscribeObjAdded(
      std::function<void(const object_manager::protocol::ObjectInfoT &)> callback) {
    absl::MutexLock lock(&store_add_mutex_);
    add_handlers_.push_back(std::move(callback));
  }

  /// Subscribe to notifications of objects deleted from local store.
  ///
  /// \param callback A callback expecting an ObjectID.
  void SubscribeObjDeleted(std::function<void(const ray::ObjectID &)> callback) {
    absl::MutexLock lock(&store_remove_mutex_);
    rem_handlers_.push_back(std::move(callback));
  }

  /// Support for rebroadcasting object add/rem events.
  void ProcessStoreAdd(const object_manager::protocol::ObjectInfoT &object_info) {
    // TODO(suquark): Use strand in boost asio to enforce sequential execution.
    absl::MutexLock lock(&store_add_mutex_);
    for (auto &handler : add_handlers_) {
      main_service_->post([handler, object_info]() { handler(object_info); });
    }
    num_adds_processed_++;
  }

  void ProcessStoreRemove(const ObjectID &object_id) {
    absl::MutexLock lock(&store_remove_mutex_);
    for (auto &handler : rem_handlers_) {
      main_service_->post([handler, object_id]() { handler(object_id); });
    }
    num_removes_processed_++;
  }

  /// Returns debug string for class.
  ///
  /// \return string.
  std::string DebugString() const {
    std::stringstream result;
    result << "ObjectStoreNotificationManager:";
    result << "\n- num adds processed: " << num_adds_processed_;
    result << "\n- num removes processed: " << num_removes_processed_;
    return result.str();
  }

 private:
  /// Weak reference to main service. We ensure this object is destroyed before
  /// main_service_ is stopped.
  boost::asio::io_service *main_service_;
  std::vector<std::function<void(const object_manager::protocol::ObjectInfoT &)>>
      add_handlers_;
  std::vector<std::function<void(const ray::ObjectID &)>> rem_handlers_;
  absl::Mutex store_add_mutex_;
  absl::Mutex store_remove_mutex_;
  int64_t num_adds_processed_;
  int64_t num_removes_processed_;
};

}  // namespace ray

#endif  // RAY_OBJECT_STORE_NOTIFICATION_MANAGER_H
