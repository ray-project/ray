// Copyright 2020-2021 The Ray Authors.
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

#include "ray/raylet/scheduling/scheduler_resource_reporter.h"

namespace ray {
namespace raylet {

SchedulerResourceReporter::SchedulerResourceReporter(
    const absl::flat_hash_map<
        SchedulingClass, std::deque<std::shared_ptr<internal::Work>>> &tasks_to_schedule,
    const absl::flat_hash_map<
        SchedulingClass, std::deque<std::shared_ptr<internal::Work>>> &tasks_to_dispatch,
    const absl::flat_hash_map<
        SchedulingClass, std::deque<std::shared_ptr<internal::Work>>> &infeasible_tasks,
    const absl::flat_hash_map<SchedulingClass, absl::flat_hash_map<WorkerID, int64_t>>
        &backlog_tracker)
    : max_resource_shapes_per_load_report_(
          RayConfig::instance().max_resource_shapes_per_load_report()),
      tasks_to_schedule_(tasks_to_schedule),
      tasks_to_dispatch_(tasks_to_dispatch),
      infeasible_tasks_(infeasible_tasks),
      backlog_tracker_(backlog_tracker) {}

int64_t SchedulerResourceReporter::TotalBacklogSize(
    SchedulingClass scheduling_class) const {
  auto backlog_it = backlog_tracker_.find(scheduling_class);
  if (backlog_it == backlog_tracker_.end()) {
    return 0;
  }

  int64_t sum = 0;
  for (const auto &worker_id_and_backlog_size : backlog_it->second) {
    sum += worker_id_and_backlog_size.second;
  }
  return sum;
}

void SchedulerResourceReporter::FillResourceUsage(
    rpc::ResourcesData &data,
    const std::shared_ptr<SchedulingResources> &last_reported_resources) const {
  if (max_resource_shapes_per_load_report_ == 0) {
    return;
  }
  auto resource_loads = data.mutable_resource_load();
  auto resource_load_by_shape =
      data.mutable_resource_load_by_shape()->mutable_resource_demands();

  int num_reported = 0;
  int64_t skipped_requests = 0;

  for (const auto &pair : tasks_to_schedule_) {
    const auto &scheduling_class = pair.first;
    if (num_reported++ >= max_resource_shapes_per_load_report_ &&
        max_resource_shapes_per_load_report_ >= 0) {
      // TODO (Alex): It's possible that we skip a different scheduling key which contains
      // the same resources.
      skipped_requests++;
      break;
    }
    const auto &resources =
        TaskSpecification::GetSchedulingClassDescriptor(scheduling_class)
            .resource_set.GetResourceMap();
    const auto &queue = pair.second;
    const auto &count = queue.size();

    auto by_shape_entry = resource_load_by_shape->Add();

    for (const auto &resource : resources) {
      // Add to `resource_loads`.
      const auto &label = resource.first;
      const auto &quantity = resource.second;
      (*resource_loads)[label] += quantity * count;

      // Add to `resource_load_by_shape`.
      (*by_shape_entry->mutable_shape())[label] = quantity;
    }

    // If a task is not feasible on the local node it will not be feasible on any other
    // node in the cluster. See the scheduling policy defined by
    // ClusterResourceScheduler::GetBestSchedulableNode for more details.
    int num_ready = by_shape_entry->num_ready_requests_queued();
    by_shape_entry->set_num_ready_requests_queued(num_ready + count);
    by_shape_entry->set_backlog_size(TotalBacklogSize(scheduling_class));
  }

  for (const auto &pair : tasks_to_dispatch_) {
    const auto &scheduling_class = pair.first;
    if (num_reported++ >= max_resource_shapes_per_load_report_ &&
        max_resource_shapes_per_load_report_ >= 0) {
      // TODO (Alex): It's possible that we skip a different scheduling key which contains
      // the same resources.
      skipped_requests++;
      break;
    }
    const auto &resources =
        TaskSpecification::GetSchedulingClassDescriptor(scheduling_class)
            .resource_set.GetResourceMap();
    const auto &queue = pair.second;
    const auto &count = queue.size();

    auto by_shape_entry = resource_load_by_shape->Add();

    for (const auto &resource : resources) {
      // Add to `resource_loads`.
      const auto &label = resource.first;
      const auto &quantity = resource.second;
      (*resource_loads)[label] += quantity * count;

      // Add to `resource_load_by_shape`.
      (*by_shape_entry->mutable_shape())[label] = quantity;
    }
    int num_ready = by_shape_entry->num_ready_requests_queued();
    by_shape_entry->set_num_ready_requests_queued(num_ready + count);
    by_shape_entry->set_backlog_size(TotalBacklogSize(scheduling_class));
  }

  for (const auto &pair : infeasible_tasks_) {
    const auto &scheduling_class = pair.first;
    if (num_reported++ >= max_resource_shapes_per_load_report_ &&
        max_resource_shapes_per_load_report_ >= 0) {
      // TODO (Alex): It's possible that we skip a different scheduling key which contains
      // the same resources.
      skipped_requests++;
      break;
    }
    const auto &resources =
        TaskSpecification::GetSchedulingClassDescriptor(scheduling_class)
            .resource_set.GetResourceMap();
    const auto &queue = pair.second;
    const auto &count = queue.size();

    auto by_shape_entry = resource_load_by_shape->Add();
    for (const auto &resource : resources) {
      // Add to `resource_loads`.
      const auto &label = resource.first;
      const auto &quantity = resource.second;
      (*resource_loads)[label] += quantity * count;

      // Add to `resource_load_by_shape`.
      (*by_shape_entry->mutable_shape())[label] = quantity;
    }

    // If a task is not feasible on the local node it will not be feasible on any other
    // node in the cluster. See the scheduling policy defined by
    // ClusterResourceScheduler::GetBestSchedulableNode for more details.
    int num_infeasible = by_shape_entry->num_infeasible_requests_queued();
    by_shape_entry->set_num_infeasible_requests_queued(num_infeasible + count);
    by_shape_entry->set_backlog_size(TotalBacklogSize(scheduling_class));
  }

  if (skipped_requests > 0) {
    RAY_LOG(INFO) << "More than " << max_resource_shapes_per_load_report_
                  << " scheduling classes. Some resource loads may not be reported to "
                     "the autoscaler.";
  }

  if (RayConfig::instance().enable_light_weight_resource_report()) {
    // Check whether resources have been changed.
    absl::flat_hash_map<std::string, double> local_resource_map(
        data.resource_load().begin(), data.resource_load().end());
    ResourceSet local_resource(local_resource_map);
    if (last_reported_resources == nullptr ||
        !last_reported_resources->GetLoadResources().IsEqual(local_resource)) {
      data.set_resource_load_changed(true);
    }
  } else {
    data.set_resource_load_changed(true);
  }
}

}  // namespace raylet
}  // namespace ray
