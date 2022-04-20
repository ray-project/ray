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

#include "ray/gcs/gcs_server/gcs_init_data.h"

namespace ray {
namespace gcs {
void GcsInitData::AsyncLoad(const EmptyCallback &on_done) {
  // There are 6 kinds of table data need to be loaded.
  auto count_down = std::make_shared<int>(6);
  auto on_load_finished = [count_down, on_done] {
    if (--(*count_down) == 0) {
      if (on_done) {
        on_done();
      }
    }
  };

  AsyncLoadJobTableData(on_load_finished);

  AsyncLoadNodeTableData(on_load_finished);

  AsyncLoadResourceTableData(on_load_finished);

  AsyncLoadActorTableData(on_load_finished);

  AsyncLoadActorTaskSpecTableData(on_load_finished);

  AsyncLoadPlacementGroupTableData(on_load_finished);
}

void GcsInitData::AsyncLoadJobTableData(const EmptyCallback &on_done) {
  RAY_LOG(INFO) << "Loading job table data.";
  auto load_job_table_data_callback =
      [this, on_done](absl::flat_hash_map<JobID, rpc::JobTableData> &&result) {
        job_table_data_ = std::move(result);
        RAY_LOG(INFO) << "Finished loading job table data, size = "
                      << job_table_data_.size();
        on_done();
      };
  RAY_CHECK_OK(gcs_table_storage_->JobTable().GetAll(load_job_table_data_callback));
}

void GcsInitData::AsyncLoadNodeTableData(const EmptyCallback &on_done) {
  RAY_LOG(INFO) << "Loading node table data.";
  auto load_node_table_data_callback =
      [this, on_done](absl::flat_hash_map<NodeID, rpc::GcsNodeInfo> &&result) {
        node_table_data_ = std::move(result);
        RAY_LOG(INFO) << "Finished loading node table data, size = "
                      << node_table_data_.size();
        on_done();
      };
  RAY_CHECK_OK(gcs_table_storage_->NodeTable().GetAll(load_node_table_data_callback));
}

void GcsInitData::AsyncLoadResourceTableData(const EmptyCallback &on_done) {
  RAY_LOG(INFO) << "Loading cluster resources table data.";
  auto load_resource_table_data_callback =
      [this, on_done](absl::flat_hash_map<NodeID, rpc::ResourceMap> &&result) {
        resource_table_data_ = std::move(result);
        RAY_LOG(INFO) << "Finished loading cluster resources table data, size = "
                      << resource_table_data_.size();
        on_done();
      };
  RAY_CHECK_OK(
      gcs_table_storage_->NodeResourceTable().GetAll(load_resource_table_data_callback));
}

void GcsInitData::AsyncLoadPlacementGroupTableData(const EmptyCallback &on_done) {
  RAY_LOG(INFO) << "Loading placement group table data.";
  auto load_placement_group_table_data_callback =
      [this, on_done](
          absl::flat_hash_map<PlacementGroupID, rpc::PlacementGroupTableData> &&result) {
        placement_group_table_data_ = std::move(result);
        RAY_LOG(INFO) << "Finished loading placement group table data, size = "
                      << placement_group_table_data_.size();
        on_done();
      };
  RAY_CHECK_OK(gcs_table_storage_->PlacementGroupTable().GetAll(
      load_placement_group_table_data_callback));
}

void GcsInitData::AsyncLoadActorTableData(const EmptyCallback &on_done) {
  RAY_LOG(INFO) << "Loading actor table data.";
  auto load_actor_table_data_callback =
      [this, on_done](absl::flat_hash_map<ActorID, ActorTableData> &&result) {
        actor_table_data_ = std::move(result);
        RAY_LOG(INFO) << "Finished loading actor table data, size = "
                      << actor_table_data_.size();
        on_done();
      };
  RAY_CHECK_OK(gcs_table_storage_->ActorTable().AsyncRebuildIndexAndGetAll(
      load_actor_table_data_callback));
}

void GcsInitData::AsyncLoadActorTaskSpecTableData(const EmptyCallback &on_done) {
  RAY_LOG(INFO) << "Loading actor task spec table data.";
  auto load_actor_task_spec_table_data_callback =
      [this, on_done](const absl::flat_hash_map<ActorID, TaskSpec> &result) {
        actor_task_spec_table_data_ = std::move(result);
        RAY_LOG(INFO) << "Finished loading actor task spec table data, size = "
                      << actor_task_spec_table_data_.size();
        on_done();
      };
  RAY_CHECK_OK(gcs_table_storage_->ActorTaskSpecTable().GetAll(
      load_actor_task_spec_table_data_callback));
}

}  // namespace gcs
}  // namespace ray