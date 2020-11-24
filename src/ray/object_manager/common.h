
#pragma once

#include <boost/asio.hpp>
#include <functional>

#include "ray/common/id.h"
#include "ray/object_manager/format/object_manager_generated.h"

namespace ray {

/// A callback to asynchronously spill objects when space is needed. The
/// callback returns the amount of space still needed after the spilling is
/// complete.
using SpillObjectsCallback = std::function<int64_t(int64_t num_bytes_required)>;

/// A callback to call when space has been released.
using SpaceReleasedCallback = std::function<void()>;

struct PullRequest {
  PullRequest() : retry_timer(nullptr), timer_set(false), client_locations() {}
  std::unique_ptr<boost::asio::deadline_timer> retry_timer;
  bool timer_set;
  std::vector<NodeID> client_locations;
};

struct ObjectManagerConfig {
  /// The port that the object manager should use to listen for connections
  /// from other object managers. If this is 0, the object manager will choose
  /// its own port.
  int object_manager_port;
  /// The time in milliseconds to wait before retrying a pull
  /// that fails due to client id lookup.
  unsigned int pull_timeout_ms;
  /// Object chunk size, in bytes
  uint64_t object_chunk_size;
  /// Max object push bytes in flight.
  uint64_t max_bytes_in_flight;
  /// The store socket name.
  std::string store_socket_name;
  /// The time in milliseconds to wait until a Push request
  /// fails due to unsatisfied local object. Special value:
  /// Negative: waiting infinitely.
  /// 0: giving up retrying immediately.
  int push_timeout_ms;
  /// Number of threads of rpc service
  /// Send and receive request in these threads
  int rpc_service_threads_number;
  /// Initial memory allocation for store.
  int64_t object_store_memory = -1;
  /// The directory for shared memory files.
  std::string plasma_directory;
  /// Enable huge pages.
  bool huge_pages;
};

struct LocalObjectInfo {
  /// Information from the object store about the object.
  object_manager::protocol::ObjectInfoT object_info;
};

using RestoreSpilledObjectCallback = std::function<void(
    const ObjectID &, const std::string &, std::function<void(const ray::Status &)>)>;

}  // namespace ray
