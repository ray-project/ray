
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
      NodeID &self_node_id, const std::function<bool(const ObjectID &)> object_is_local,
      const std::function<void(const ObjectID &, const NodeID &)> send_pull_request,
      const std::function<int(int)> get_rand_int,
      const RestoreSpilledObjectCallback restore_spilled_object);

  bool Pull(const ObjectID &object_id, const rpc::Address &owner_address);

  void OnLocationChange(const ObjectID &object_id,
                        const std::unordered_set<NodeID> &client_ids,
                        const std::string &spilled_url);

  bool CancelPull(const ObjectID &object_id);

  void Tick();

  int NumRequests() const;

 private:
  NodeID self_node_id_;

  const std::function<bool(const ObjectID &)> object_is_local_;

  /// The objects that this object manager is currently trying to fetch from
  /// remote object managers.
  std::unordered_map<ObjectID, PullRequest> pull_requests_;

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
