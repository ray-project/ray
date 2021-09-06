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

#include "ray/raylet/io_spill_worker_pool.h"

namespace ray {
namespace raylet {

IOSpillWorkerPool::IOSpillWorkerPool(instrumented_io_context &worker_pooler_executor,
                                     IOWorkerPoolInterface &io_worker_pool)
    : executor_(worker_pooler_executor), io_worker_pool_(io_worker_pool) {}

void IOSpillWorkerPool::StartSpillWorker(
    std::vector<ObjectID> objects_to_spill, std::vector<ray::rpc::Address> object_owners,
    std::function<void(ray::Status, std::vector<std::string>)> spill_callback) {
  executor_.post([this, objects_to_spill = std::move(objects_to_spill),
                  object_owners = std:: : move(object_owners),
                  spill_callback = std::move(spill_callback)]() {
    StartSpillWorkerImpl(objects_to_spill, object_owners, spill_callback);
  });
}

void IOSpillWorkerPool::StartDeleteWorker(
    std::vector<std::string> files_to_delete,
    std::function<void(ray::Status)> delete_callback) {
  executor_.post([this, files_to_delete = std::move(files_to_delete),
                  delete_callback = std::move(delete_callback)]() {
    StartDeleteWorkerImpl(files_to_delete, delete_callback);
  });
}

void IOSpillWorkerPool::StartSpillWorkerImpl(
    std::vector<ObjectID> objects_to_spill, std::vector<ray::rpc::Address> object_owners,
    std::function<void(ray::Status, std::vector<std::string>)> spill_callback) {
  io_worker_pool_.PopSpillWorker(
      [this, objects_to_spill = std::move(objects_to_spill),
       object_owners = std::move(object_owners),
       callback = std::move(spill_callback)](std::shared_ptr<WorkerInterface> io_worker) {
        RAY_CHECK(objects_to_spill.size() == object_owners.size());

        rpc::SpillObjectsRequest request;
        for (size_t i = 0; i < objects_to_spill.size(); i++) {
          const auto &object_id = objects_to_spill.at(i);
          const auto &owner = object_owners.at(i);
          RAY_LOG(DEBUG) << "Sending spill request for object " << object_id;
          auto ref = request.add_object_refs_to_spill();
          ref->set_object_id(object_id.Binary());
          ref->mutable_owner_address()->CopyFrom(owner);
        }

        io_worker->rpc_client()->SpillObjects(
            request, [this, objects_to_spill = std::move(objects_to_spill),
                      callback = std::move(callback), io_worker](
                         const ray::Status &status, const rpc::SpillObjectsReply &reply) {
              io_worker_pool_.PushSpillWorker(io_worker);
              std::vector<std::string> spilled_urls;
              for (int64_t i = 0; i < reply.spilled_objects_url_size(); ++i) {
                spilled_urls.push_back(reply.spilled_objects_url(i));
              }
              callback(status, std::move(spilled_urls));
            });
      });
}

void IOSpillWorkerPool::StartDeleteWorkerImpl(
    std::vector<std::string> files_to_delete,
    std::function<void(ray::Status)> delete_callback) {
  io_worker_pool_.PopDeleteWorker([this, files_to_delete = std::move(files_to_delete)](
                                      std::shared_ptr<WorkerInterface> io_worker) {
    RAY_LOG(DEBUG) << "Sending delete spilled object request. Length: "
                   << files_to_delete.size();
    rpc::DeleteSpilledObjectsRequest request;
    request.add_spilled_objects_url(std::move(files_to_delete));
    io_worker->rpc_client()->DeleteSpilledObjects(
        request, [this, io_worker](const ray::Status &status,
                                   const rpc::DeleteSpilledObjectsReply /* unused */) {
          io_worker_pool_.PushDeleteWorker(io_worker);
          delete_callback(status);
        });
  });
}

}  // namespace raylet
}  // namespace ray