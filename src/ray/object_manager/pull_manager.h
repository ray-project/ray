
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

  PullManager(std::unordered_map<ObjectID, LocalObjectInfo> *local_objects,
              std::unordered_map<ObjectID, PullRequest> *pull_requests);

  Status Pull(const ObjectID &object_id,
              const rpc::Address &owner_address);

private:
  /// A weak reference to the object manager's mapping of locally available objects.
  std::unordered_map<ObjectID, LocalObjectInfo> *local_objects_;

  /// A weak reference to the object manager's tracking of in progress object pulls.
  std::unordered_map<ObjectID, PullRequest> pull_requests_;

};
}
