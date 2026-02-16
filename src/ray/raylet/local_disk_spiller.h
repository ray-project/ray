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

#pragma once

#include <atomic>
#include <boost/asio/executor_work_guard.hpp>
#include <boost/asio/io_context.hpp>
#include <functional>
#include <memory>
#include <string>
#include <thread>
#include <vector>

#include "ray/common/asio/instrumented_io_context.h"
#include "ray/common/buffer.h"
#include "ray/common/id.h"
#include "ray/raylet/object_spiller.h"

namespace ray {

namespace raylet {

/// Callback to create an object in the plasma store from restored data.
/// Must be called on the main thread (plasma store is not thread-safe).
using RestoreObjectToPlasmaFn = std::function<Status(const ObjectID &object_id,
                                                     const rpc::Address &owner_address,
                                                     std::shared_ptr<Buffer> data,
                                                     std::shared_ptr<Buffer> metadata)>;

/// Implements ObjectSpillerInterface using local-disk file I/O on a dedicated
/// thread pool. Spill/restore/delete operations run on IO threads; completion
/// callbacks are posted back to the main io_service.
class LocalDiskSpiller : public ObjectSpillerInterface {
 public:
  /// Construct a LocalDiskSpiller.
  ///
  /// \param spill_directories Base directories for spilled object files.
  ///     A subdirectory "ray_spilled_objects_{node_id_hex}" will be created in each.
  /// \param node_id The node ID, used for the subdirectory name.
  /// \param num_io_threads Number of IO threads for the worker pool.
  /// \param main_io_service The main event loop, used to post completion callbacks.
  /// \param restore_to_plasma Callback to restore an object into the plasma store.
  LocalDiskSpiller(const std::vector<std::string> &spill_directories,
                   const NodeID &node_id,
                   int num_io_threads,
                   instrumented_io_context &main_io_service,
                   RestoreObjectToPlasmaFn restore_to_plasma);

  ~LocalDiskSpiller() override;

  void SpillObjects(const std::vector<ObjectID> &object_ids,
                    const std::vector<const RayObject *> &objects,
                    const std::vector<rpc::Address> &owner_addresses,
                    std::function<void(const Status &, std::vector<std::string> urls)>
                        callback) override;

  void RestoreSpilledObject(
      const ObjectID &object_id,
      const std::string &object_url,
      std::function<void(const Status &, int64_t bytes_restored)> callback) override;

  void DeleteSpilledObjects(const std::vector<std::string> &urls,
                            std::function<void(const Status &)> callback) override;

 private:
  /// Write a uint64_t in little-endian format to a stream.
  static void WriteUint64LE(std::ostream &os, uint64_t value);

  /// Get the next spill directory in round-robin order.
  const std::string &NextSpillDirectory();

  /// The directories to spill objects to (already includes the per-node subdirectory).
  std::vector<std::string> spill_dirs_;

  /// Atomic counter for round-robin directory selection.
  std::atomic<uint64_t> dir_index_{0};

  /// Atomic counter for unique file names.
  std::atomic<uint64_t> file_counter_{0};

  /// The main io_service to post callbacks back to.
  instrumented_io_context &main_io_service_;

  /// Callback to restore an object into the plasma store.
  RestoreObjectToPlasmaFn restore_to_plasma_;

  /// IO thread pool.
  boost::asio::io_context io_context_;
  boost::asio::executor_work_guard<boost::asio::io_context::executor_type> work_guard_;
  std::vector<std::thread> io_threads_;
};

}  // namespace raylet

}  // namespace ray
