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

#include "ray/object_manager/plasma/object_spill_manager.h"

#include <regex>

namespace plasma {

ObjectSpillManager::ObjectSpillManager(
    instrumented_io_context &io_context,
    std::unique_ptr<ISpillWorkerPool> spill_worker_pool, int64_t max_active_worker)
    : io_context_(io_context),
      spill_worker_pool_(std::move(spill_worker_pool)),
      kMaxActiveWorker(max_active_worker),
      num_active_workers_(0),
      spilled_objects_(),
      spilled_files_() {}

bool ObjectSpillManager::SubmitSpillTask(
    std::vector<ObjectID> objects_to_spill, std::vector<ray::rpc::Address> object_owners,
    std::function<void(ray::Status status, std::vector<ObjectID>)> callback) {
  if (!CanSubmitSpillTask()) {
    return false;
  }

  num_active_workers_ += 1;
  spill_worker_pool_->StartSpillWorker(
      objects_to_spill, std::move(object_owners),
      [this, objects_to_spill = std::move(objects_to_spill),
       callback = std::move(callback)](ray::Status status,
                                       std::vector<std::string> spilled_urls) {
        io_context_.post([this, objects_to_spill = std::move(objects_to_spill),
                          callback = std::move(callback), status,
                          spilled_urls = std::move(spilled_urls)]() {
          OnSpillTaskFinished(std::move(objects_to_spill), status,
                              std::move(spilled_urls), std::move(callback));
        });
      });
  return true;
}

bool ObjectSpillManager::CanSubmitSpillTask() const {
  return num_active_workers_ < kMaxActiveWorker;
}

/// Delete a spilled object from external storage.
void ObjectSpillManager::DeleteSpilledObject(const ObjectID &object_id) {
  if (!spilled_objects_.contains(object_id)) {
    RAY_LOG(WARNING) << "Deleting an non-existing spilled object " << object_id;
    return;
  }

  auto uri = spilled_objects_.at(object_id);
  spilled_objects_.erase(object_id);

  RAY_CHECK(spilled_files_.contains(uri.file));
  spilled_files_[uri.file]--;
  if (spilled_files_[uri.file] > 0) {
    return;
  }

  spilled_files_.erase(uri.file);
  DeletedSpilledFile(uri.file);
}

void ObjectSpillManager::DeletedSpilledFile(const std::string &file_to_remove) {
  spill_worker_pool_->StartDeleteWorker({file_to_remove}, [](auto status) {
    if (!status.ok()) {
      RAY_LOG(ERROR) << "Failed to send delete spilled object request: "
                     << status.ToString();
    }
  });
}

absl::optional<std::string> ObjectSpillManager::GetSpilledUrl(
    const ObjectID &object_id) const {
  auto it = spilled_objects_.find(object_id);
  if (it == spilled_objects_.end()) {
    return absl::nullopt;
  }
  return it->second.ToString();
}

bool ObjectSpillManager::IsSpillingInProgress() const { return num_active_workers_ > 0; }

void ObjectSpillManager::OnSpillTaskFinished(
    std::vector<ObjectID> objects_to_spill, ray::Status status,
    std::vector<std::string> spilled_urls,
    std::function<void(ray::Status status, std::vector<ObjectID>)>
        task_finished_callback) {
  num_active_workers_ -= 1;
  if (!status.ok()) {
    task_finished_callback(status, {});
    return;
  }
  std::vector<ObjectID> spilled_objects;
  for (size_t i = 0; i < spilled_urls.size(); ++i) {
    auto uri = ParseSpilledURI(spilled_urls.at(i));
    if (!uri.has_value()) {
      RAY_LOG(WARNING) << "bad";
      continue;
    }
    auto &object_id = objects_to_spill.at(i);
    if (spilled_objects_.contains(object_id)) {
      RAY_LOG(WARNING) << "bad";
      continue;
    }
    spilled_objects_[object_id] = uri.value();
    spilled_files_[uri->file]++;
    spilled_objects.push_back(object_id);
  }
  task_finished_callback(status, std::move(spilled_objects));
}

absl::optional<ObjectSpillManager::SpilledObjectURI> ObjectSpillManager::ParseSpilledURI(
    const std::string &object_url) {
  static const std::regex kObjectURLPattern("^(.*)\\?offset=(\\d+)&size=(\\d+)$");
  std::smatch match_groups;
  if (!std::regex_match(object_url, match_groups, kObjectURLPattern) ||
      match_groups.size() != 4) {
    return absl::nullopt;
  }
  SpilledObjectURI uri;
  uri.file = match_groups[1].str();
  try {
    uri.offset = std::stoll(match_groups[2].str());
    uri.size = std::stoll(match_groups[3].str());
    if (uri.offset < 0 || uri.size < 0) {
      RAY_LOG(ERROR) << "Offset and size can't be negative. offset: " << uri.offset
                     << ", size: " << uri.size;
      return absl::nullopt;
    }
  } catch (...) {
    RAY_LOG(ERROR) << "Failed to parse offset: " << match_groups[2].str()
                   << " and size: " << match_groups[3].str();
    return absl::nullopt;
  }
  return std::move(uri);
}

}  // namespace plasma