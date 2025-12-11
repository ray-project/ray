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

#include "ray/gcs/gcs_init_data.h"

#include <memory>
#include <utility>
#include <vector>

namespace ray {
namespace gcs {
void GcsInitData::AsyncLoad(Postable<void()> on_done) {
  // There are 7 kinds of table data need to be loaded.
  auto count_down = std::make_shared<int>(7);
  auto on_load_finished = Postable<void()>(
      [count_down, on_done]() mutable {
        if (--(*count_down) == 0) {
          std::move(on_done).Dispatch("GcsInitData::AsyncLoad");
        }
      },
      on_done.io_context());

  AsyncLoadJobTableData(on_load_finished);

  AsyncLoadNodeTableData(on_load_finished);

  AsyncLoadActorTableData(on_load_finished);

  AsyncLoadActorTaskSpecTableData(on_load_finished);

  AsyncLoadPlacementGroupTableData(on_load_finished);

  AsyncLoadVirtualClusterTableData(on_load_finished);

  AsyncLoadWorkerTableData(on_load_finished);
}

void GcsInitData::AsyncLoadJobTableData(Postable<void()> on_done) {
  RAY_LOG(INFO) << "Loading job table data.";
  gcs_table_storage_.JobTable().GetAll(std::move(on_done).TransformArg(
      [this](absl::flat_hash_map<JobID, rpc::JobTableData> result) {
        job_table_data_ = std::move(result);
        RAY_LOG(INFO) << "Finished loading job table data, size = "
                      << job_table_data_.size();
      }));
}

void GcsInitData::AsyncLoadNodeTableData(Postable<void()> on_done) {
  RAY_LOG(INFO) << "Loading node table data.";
  gcs_table_storage_.NodeTable().GetAll(std::move(on_done).TransformArg(
      [this](absl::flat_hash_map<NodeID, rpc::GcsNodeInfo> result) {
        node_table_data_ = std::move(result);
        RAY_LOG(INFO) << "Finished loading node table data, size = "
                      << node_table_data_.size();
      }));
}

void GcsInitData::AsyncLoadPlacementGroupTableData(Postable<void()> on_done) {
  RAY_LOG(INFO) << "Loading placement group table data.";
  gcs_table_storage_.PlacementGroupTable().GetAll(std::move(on_done).TransformArg(
      [this](absl::flat_hash_map<PlacementGroupID, rpc::PlacementGroupTableData> result) {
        placement_group_table_data_ = std::move(result);
        RAY_LOG(INFO) << "Finished loading placement group table data, size = "
                      << placement_group_table_data_.size();
      }));
}

void GcsInitData::AsyncLoadActorTableData(Postable<void()> on_done) {
  RAY_LOG(INFO) << "Loading actor table data.";
  gcs_table_storage_.ActorTable().AsyncRebuildIndexAndGetAll(
      std::move(on_done).TransformArg(
          [this](absl::flat_hash_map<ActorID, rpc::ActorTableData> result) {
            actor_table_data_ = std::move(result);
            RAY_LOG(INFO) << "Finished loading actor table data, size = "
                          << actor_table_data_.size();
          }));
}

void GcsInitData::AsyncLoadActorTaskSpecTableData(Postable<void()> on_done) {
  RAY_LOG(INFO) << "Loading actor task spec table data.";
  gcs_table_storage_.ActorTaskSpecTable().GetAll(std::move(on_done).TransformArg(
      [this](absl::flat_hash_map<ActorID, rpc::TaskSpec> result) -> void {
        actor_task_spec_table_data_ = std::move(result);
        RAY_LOG(INFO) << "Finished loading actor task spec table data, size = "
                      << actor_task_spec_table_data_.size();
      }));
}

void GcsInitData::AsyncLoadVirtualClusterTableData(Postable<void()> on_done) {
  RAY_LOG(INFO) << "Loading virtual cluster table data.";
  gcs_table_storage_.VirtualClusterTable().GetAll(std::move(on_done).TransformArg(
      [this](absl::flat_hash_map<VirtualClusterID, rpc::VirtualClusterTableData> result)
          -> void {
        virtual_cluster_table_data_ = std::move(result);
        RAY_LOG(INFO) << "Finished loading virtual cluster table data, size = "
                      << virtual_cluster_table_data_.size();
      }));
}

void GcsInitData::AsyncLoadWorkerTableData(Postable<void()> on_done) {
  RAY_LOG(INFO) << "Loading worker table data.";
  gcs_table_storage_.WorkerTable().GetAll(std::move(on_done).TransformArg(
      [this](absl::flat_hash_map<WorkerID, rpc::WorkerTableData> result) -> void {
        worker_table_data_ = std::move(result);
        RAY_LOG(INFO) << "Finished loading worker table data, size = "
                      << worker_table_data_.size();
      }));
}

bool OnInitializeActorShouldLoad(const ray::gcs::GcsInitData &gcs_init_data,
                                 ray::ActorID actor_id) {
  const auto &jobs = gcs_init_data.Jobs();
  const auto &actors = gcs_init_data.Actors();
  const auto &actor_task_specs = gcs_init_data.ActorTaskSpecs();

  const auto &actor_table_data = actors.find(actor_id);
  if (actor_table_data == actors.end()) {
    return false;
  }
  if (actor_table_data->second.state() == ray::rpc::ActorTableData::DEAD &&
      !ray::gcs::IsActorRestartable(actor_table_data->second)) {
    return false;
  }

  const auto &actor_task_spec = ray::map_find_or_die(actor_task_specs, actor_id);
  ray::ActorID root_detached_actor_id =
      ray::TaskSpecification(actor_task_spec).RootDetachedActorId();
  if (root_detached_actor_id.IsNil()) {
    // owner is job, NOT detached actor, should die with job
    auto job_iter = jobs.find(actor_id.JobId());
    return job_iter != jobs.end() && !job_iter->second.is_dead();
  } else if (actor_id == root_detached_actor_id) {
    // owner is itself, just live on
    return true;
  } else {
    // owner is another detached actor, should die with the owner actor
    // Root detached actor can be dead only if state() == DEAD.
    auto root_detached_actor_iter = actors.find(root_detached_actor_id);
    return root_detached_actor_iter != actors.end() &&
           root_detached_actor_iter->second.state() != ray::rpc::ActorTableData::DEAD;
  }
};

}  // namespace gcs
}  // namespace ray
