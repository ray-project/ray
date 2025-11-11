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
  // There are 5 kinds of table data need to be loaded.
  auto count_down = std::make_shared<int>(5);
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

}  // namespace gcs
}  // namespace ray
