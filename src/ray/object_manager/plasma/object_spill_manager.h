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

#include <string>
#include <vector>

#include "absl/container/flat_hash_map.h"
#include "absl/strings/str_format.h"
#include "absl/types/optional.h"
#include "ray/common/asio/instrumented_io_context.h"
#include "ray/common/id.h"
#include "ray/object_manager/plasma/common.h"
#include "ray/object_manager/plasma/spill_worker_pool.h"
#include "src/ray/protobuf/common.pb.h"

namespace plasma {

class ObjectSpillManager {
 public:
  ObjectSpillManager(instrumented_io_context &callback_executor,
                     std::unique_ptr<ISpillWorkerPool> spill_worker_pool,
                     int64_t max_active_worker);

  /// Spill a list of objects in an asynchronous task.
  ///
  /// \param objects_to_spill List of objects to spill.
  /// \param task_finished_callback The callback once the async spill task finishes.
  ///   - If the spill task succeed it returns Status::OK() and a map from object_id
  ///     to the spilled location.
  ///   - If the spill task failed, it returns failure Status and a map from object_id
  ///     to empty strings.
  /// \return wether the spill task is successfully scheduled.
  ///   - It returns true if the spill task is successfuly scheduled,
  ///     where the callback is guarantee to be called.
  ///   - Otherwise it returns false and the callback will not be called
  bool SubmitSpillTask(std::vector<ObjectID> objects_to_spill,
                       std::vector<ray::rpc::Address> object_owners,
                       std::function<void(ray::Status status, std::vector<ObjectID>)>
                           task_finished_callback);

  /// Wether we can submit another spill task.
  bool CanSubmitSpillTask() const;

  /// Delete a spilled object from external storage.
  void DeleteSpilledObject(const ObjectID &object_id);

  absl::optional<std::string> GetSpilledUrl(const ObjectID &object_id) const;

  bool IsSpillingInProgress() const;

 private:
  struct SpilledObjectURI {
    std::string file;
    int64_t offset;
    int64_t size;

    std::string ToString() const {
      return absl::StrFormat("%s?offset=%d&size=%d", file, offset, size);
    }
  };

  void OnSpillTaskFinished(std::vector<ObjectID> objects_to_spill, ray::Status status,
                           std::vector<std::string> spilled_urls,
                           std::function<void(ray::Status status, std::vector<ObjectID>)>
                               task_finished_callback);

  absl::optional<SpilledObjectURI> ParseSpilledURI(const std::string &s);

  void DeletedSpilledFile(const std::string &spilled_file);

 private:
  instrumented_io_context &io_context_;
  std::unique_ptr<ISpillWorkerPool> spill_worker_pool_;
  const int64_t kMaxActiveWorker;
  int64_t num_active_workers_;

  absl::flat_hash_map<ObjectID, SpilledObjectURI> spilled_objects_;
  absl::flat_hash_map<std::string, int64_t> spilled_files_;
};

}  // namespace plasma