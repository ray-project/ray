
#pragma once

#include <boost/asio.hpp>
#include <boost/asio/error.hpp>
#include <boost/bind.hpp>
#include <map>

#include "absl/container/flat_hash_map.h"
#include "absl/container/flat_hash_set.h"
#include "absl/time/clock.h"
#include "ray/common/id.h"
#include "ray/common/ray_config.h"
#include "ray/common/status.h"
#include "ray/object_manager/common.h"
#include "ray/object_manager/format/object_manager_generated.h"
#include "ray/object_manager/notification/object_store_notification_manager_ipc.h"
#include "ray/object_manager/object_buffer_pool.h"
#include "ray/object_manager/object_directory.h"
#include "ray/object_manager/ownership_based_object_directory.h"
#include "ray/object_manager/plasma/store_runner.h"
#include "ray/rpc/object_manager/object_manager_client.h"
#include "ray/rpc/object_manager/object_manager_server.h"

namespace ray {

class PullManager {
 public:
  PullManager(
      NodeID &self_node_id, const ObjectManagerConfig &config,
      std::shared_ptr<ObjectDirectoryInterface> &object_directory,
      UniqueID &object_directory_pull_callback_id,
      std::unordered_map<ObjectID, LocalObjectInfo> *local_objects,
      std::unordered_map<ObjectID, PullRequest> *pull_requests,
      const std::function<void(const ObjectID &, const NodeID &)> &send_pull_request,
      const std::function<int(int)> &get_rand_int,
      const RestoreSpilledObjectCallback &restore_spilled_object);

  Status Pull(const ObjectID &object_id, const rpc::Address &owner_address);

 private:
  NodeID self_node_id_;
  const ObjectManagerConfig config_;
  std::shared_ptr<ObjectDirectoryInterface> object_directory_;

  UniqueID object_directory_pull_callback_id_;

  /* Note: Both of these weak references are fate-shared to the object manager, as is the
   * pull manager. Therefore we aren't worried about corruption issue. */
  /// A weak reference to the object manager's mapping of locally available objects.
  std::unordered_map<ObjectID, LocalObjectInfo> *local_objects_;
  /// A weak reference to the object manager's tracking of in progress object pulls.
  std::unordered_map<ObjectID, PullRequest> *pull_requests_;

  const std::function<void(const ObjectID &, const NodeID &)> send_pull_request_;

  const std::function<int(int)> get_rand_int_;

  const RestoreSpilledObjectCallback restore_spilled_object_;

  /// Try to Pull an object from one of its expected client locations. If there
  /// are more client locations to try after this attempt, then this method
  /// will try each of the other clients in succession, with a timeout between
  /// each attempt. If the object is received or if the Pull is Canceled before
  /// the timeout, then no more Pull requests for this object will be sent
  /// to other node managers until TryPull is called again.
  ///
  /// \param object_id The object's object id.
  /// \return Void.
  void TryPull(const ObjectID &object_id);
};
}  // namespace ray
