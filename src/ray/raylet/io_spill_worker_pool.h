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

#include "ray/object_manager/plasma/spill_worker_pool.h"
#include "ray/raylet/worker_pool.h"

namespace ray {
namespace raylet {
class IOSpillWorkerPool : plasma::ISpillWorkerPool {
 public:
  IOSpillWorkerPool(instrumented_io_context &worker_pooler_executor,
                    IOWorkerPoolInterface &io_worker_pool);

  void StartSpillWorker(
      std::vector<ObjectID> objects_to_spill,
      std::vector<ray::rpc::Address> object_owners,
      std::function<void(ray::Status, std::vector<std::string>)> spill_callback) override;

  void StartDeleteWorker(std::vector<std::string> files_to_delete,
                         std::function<void(ray::Status)> delete_callback) override;

 private:
  void StartSpillWorkerImpl(
      std::vector<ObjectID> objects_to_spill,
      std::vector<ray::rpc::Address> object_owners,
      std::function<void(ray::Status, std::vector<std::string>)> spill_callback);

  void StartDeleteWorkerImpl(
      std::vector<ObjectID> objects_to_spill,
      std::vector<ray::rpc::Address> object_owners,
      std::function<void(ray::Status, std::vector<std::string>)> spill_callback);

 private:
  instrumented_io_context &executor_;
  IOWorkerPoolInterface &io_worker_pool_;
};

}  // namespace raylet
}  // namespace ray