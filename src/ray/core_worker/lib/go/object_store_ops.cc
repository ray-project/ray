// Copyright 2025 The Ray Authors.
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

#include "object_store_ops.h"

#include <sstream>
#include <string>
#include <unordered_map>
#include <vector>

#include "ray/common/id.h"
#include "ray/common/ray_object.h"
#include "ray/core_worker/core_worker.h"
#include "ray/core_worker/core_worker_options.h"
#include "ray/core_worker/core_worker_process.h"
#include "ray/util/logging.h"

namespace ray {
namespace go {

// Static provider - defaults to DefaultObjectStoreProvider
static std::shared_ptr<IObjectStoreProvider> g_core_worker_provider =
    std::make_shared<DefaultObjectStoreProvider>();

ObjectStoreOperations &ObjectStoreOperations::GetInstance() {
  static ObjectStoreOperations instance;
  return instance;
}

void ObjectStoreOperations::SetCoreWorkerProvider(
    std::shared_ptr<IObjectStoreProvider> provider) {
  g_core_worker_provider = provider;
}

IObjectStoreProvider &ObjectStoreOperations::GetCoreWorkerProvider() {
  return *g_core_worker_provider;
}

ray::ObjectID ObjectStoreOperations::Put(const std::shared_ptr<ray::RayObject> &object) {
  auto &core_worker = GetCoreWorker();

  ray::ObjectID object_id;
  std::vector<ray::ObjectID> contained_object_ids;
  RAY_CHECK_OK(core_worker.Put(*object, contained_object_ids, &object_id));

  return object_id;
}

void ObjectStoreOperations::PutWithID(const ray::ObjectID &object_id,
                                      const std::shared_ptr<ray::RayObject> &object) {
  auto &core_worker = GetCoreWorker();

  std::vector<ray::ObjectID> contained_object_ids;
  // Register ownership for the caller-supplied ID before putting, mirroring
  // CoreWorker::Put's three-argument overload (which registers ownership when
  // it generates the ID). Without an owned entry in the reference counter, a
  // subsequent Get cannot resolve an owner and the fetch never completes.
  core_worker.AddOwnedObject(object_id,
                             contained_object_ids,
                             object->GetSize(),
                             /*add_local_ref=*/true);
  auto status =
      core_worker.Put(*object, contained_object_ids, object_id, /*pin_object=*/true);
  // Roll back the local reference on failure, matching the three-argument Put
  // overload, so a failed write does not leak a reference.
  if (!status.ok()) {
    core_worker.RemoveLocalReference(object_id);
  }
}

std::vector<std::shared_ptr<ray::RayObject>> ObjectStoreOperations::Get(
    const std::vector<ray::ObjectID> &ids, int64_t timeout_ms) {
  auto &core_worker = GetCoreWorker();

  std::vector<std::shared_ptr<ray::RayObject>> objects;
  auto status = core_worker.Get(ids, timeout_ms < 0 ? -1 : timeout_ms, objects);
  RAY_CHECK_OK(status);

  return objects;
}

std::vector<bool> ObjectStoreOperations::Wait(const std::vector<ray::ObjectID> &ids,
                                              int num_objects,
                                              int64_t timeout_ms,
                                              bool fetch_local) {
  auto &core_worker = GetCoreWorker();

  std::vector<bool> ready;
  RAY_CHECK_OK(
      core_worker.Wait(ids,
                       num_objects <= 0 ? static_cast<int>(ids.size()) : num_objects,
                       timeout_ms < 0 ? -1 : timeout_ms,
                       &ready,
                       fetch_local));

  return ready;
}

void ObjectStoreOperations::Delete(const std::vector<ray::ObjectID> &ids,
                                   bool local_only) {
  auto &core_worker = GetCoreWorker();
  RAY_CHECK_OK(core_worker.Delete(ids, local_only));
}

void ObjectStoreOperations::AddLocalReference(const ray::ObjectID &object_id) {
  auto &core_worker = GetCoreWorker();
  core_worker.AddLocalReference(object_id);
}

void ObjectStoreOperations::RemoveLocalReference(const ray::ObjectID &object_id) {
  auto &core_worker = GetCoreWorker();
  core_worker.RemoveLocalReference(object_id);
}

std::unordered_map<ray::ObjectID, std::pair<size_t, size_t>>
ObjectStoreOperations::GetAllReferenceCounts() {
  auto &core_worker = GetCoreWorker();
  return core_worker.GetAllReferenceCounts();
}

ray::rpc::Address ObjectStoreOperations::GetOwnerAddress(const ray::ObjectID &object_id) {
  auto &core_worker = GetCoreWorker();

  ray::rpc::Address owner_address;
  RAY_CHECK_OK(core_worker.GetOwnerAddress(object_id, &owner_address));

  return owner_address;
}

std::string ObjectStoreOperations::GetOwnershipInfo(const ray::ObjectID &object_id) {
  auto &core_worker = GetCoreWorker();

  ray::rpc::Address owner_address;
  std::string serialized_metadata;
  RAY_CHECK_OK(
      core_worker.GetOwnershipInfo(object_id, &owner_address, &serialized_metadata));

  return serialized_metadata;
}

void ObjectStoreOperations::RegisterOwnershipInfoAndResolveFuture(
    const ray::ObjectID &object_id,
    const ray::ObjectID &outer_object_id,
    const ray::rpc::Address &owner_address) {
  auto &core_worker = GetCoreWorker();

  core_worker.RegisterOwnershipInfoAndResolveFuture(
      object_id,
      outer_object_id.IsNil() ? object_id : outer_object_id,
      owner_address,
      "");
}

}  // namespace go
}  // namespace ray
