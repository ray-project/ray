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

#include <google/protobuf/util/json_util.h>

#include <boost/range/adaptor/filtered.hpp>
#include <boost/range/adaptor/transformed.hpp>
#include <boost/range/join.hpp>

namespace ray {
namespace raylet {
namespace {
// The max number of pending actors to report in node stats.
const int kMaxPendingActorsToReport = 20;
};  // namespace

SchedulerResourceReporter::SchedulerResourceReporter(
    const absl::flat_hash_map<SchedulingClass,
                              std::deque<std::shared_ptr<internal::Work>>>
        &tasks_to_schedule,
    const absl::flat_hash_map<SchedulingClass,
                              std::deque<std::shared_ptr<internal::Work>>>
        &infeasible_tasks,
    const ILocalTaskManager &local_task_manager)
    : max_resource_shapes_per_load_report_(
          RayConfig::instance().max_resource_shapes_per_load_report()),
      tasks_to_schedule_(tasks_to_schedule),
      tasks_to_dispatch_(local_task_manager.GetTaskToDispatch()),
      infeasible_tasks_(infeasible_tasks),
      backlog_tracker_(local_task_manager.GetBackLogTracker()) {}

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
    const std::shared_ptr<NodeResources> &last_reported_resources) const {
  if (max_resource_shapes_per_load_report_ == 0) {
    return;
  }

  auto resource_loads = data.mutable_resource_load();
  auto resource_load_by_shape =
      data.mutable_resource_load_by_shape()->mutable_resource_demands();

  int num_reported = 0;
  int64_t skipped_requests = 0;

  absl::flat_hash_set<SchedulingClass> visited;
  auto fill_resource_usage_helper = [&](const auto &range, bool is_infeasible) mutable {
    for (auto [scheduling_class, count] : range) {
      if (num_reported++ >= max_resource_shapes_per_load_report_ &&
          max_resource_shapes_per_load_report_ >= 0) {
        // TODO (Alex): It's possible that we skip a different scheduling key which
        // contains the same resources.
        skipped_requests++;
        break;
      }

      const auto &scheduling_class_descriptor =
          TaskSpecification::GetSchedulingClassDescriptor(scheduling_class);
      if ((scheduling_class_descriptor.scheduling_strategy.scheduling_strategy_case() ==
           rpc::SchedulingStrategy::SchedulingStrategyCase::
               kNodeAffinitySchedulingStrategy) &&
          !is_infeasible) {
        // Resource demands from tasks with node affinity scheduling strategy shouldn't
        // create new nodes since those tasks are intended to run with existing nodes. The
        // exception is when soft is False and there is no feasible node. In this case, we
        // should report so autoscaler can launch new nodes to unblock the tasks.
        // TODO(Alex): ideally we should report everything to autoscaler and autoscaler
        // can decide whether or not to launch new nodes based on scheduling strategies.
        // However currently scheduling strategies are not part of resource load report
        // and adding it while maintaining backward compatibility in autoscaler is not
        // trivial so we should do it during the autoscaler redesign.
        continue;
      }

      const auto &resources = scheduling_class_descriptor.resource_set.GetResourceMap();
      auto by_shape_entry = resource_load_by_shape->Add();

      for (const auto &resource : resources) {
        const auto &label = resource.first;
        const auto &quantity = resource.second;

        if (count != 0) {
          // Add to `resource_loads`.
          (*resource_loads)[label] += quantity * count;
        }
        // Add to `resource_load_by_shape`.
        (*by_shape_entry->mutable_shape())[label] = quantity;
      }

      if (is_infeasible) {
        by_shape_entry->set_num_infeasible_requests_queued(count);
      } else {
        by_shape_entry->set_num_ready_requests_queued(count);
      }

      // Backlog has already been set
      if (visited.count(scheduling_class) == 0) {
        by_shape_entry->set_backlog_size(TotalBacklogSize(scheduling_class));
        visited.insert(scheduling_class);
      }
    }
  };

  auto transform_func = [](const auto &pair) {
    return std::make_pair(pair.first, pair.second.size());
  };

  fill_resource_usage_helper(
      tasks_to_schedule_ | boost::adaptors::transformed(transform_func), false);
  fill_resource_usage_helper(
      tasks_to_dispatch_ | boost::adaptors::transformed(transform_func), false);
  fill_resource_usage_helper(
      infeasible_tasks_ | boost::adaptors::transformed(transform_func), true);
  auto backlog_tracker_range = backlog_tracker_ |
                               boost::adaptors::transformed([](const auto &pair) {
                                 return std::make_pair(pair.first, 0);
                               }) |
                               boost::adaptors::filtered([&visited](const auto &pair) {
                                 return visited.count(pair.first) == 0;
                               });

  fill_resource_usage_helper(backlog_tracker_range, false);

  if (skipped_requests > 0) {
    RAY_LOG(INFO) << "More than " << max_resource_shapes_per_load_report_
                  << " scheduling classes. Some resource loads may not be reported to "
                     "the autoscaler.";
  }

  if (RayConfig::instance().enable_light_weight_resource_report() &&
      last_reported_resources != nullptr) {
    // Check whether resources have been changed.
    absl::flat_hash_map<std::string, double> local_resource_map(
        data.resource_load().begin(), data.resource_load().end());
    ray::ResourceRequest local_resource =
        ResourceMapToResourceRequest(local_resource_map, false);
    if (last_reported_resources->load != local_resource) {
      data.set_resource_load_changed(true);
    }
  } else {
    data.set_resource_load_changed(true);
  }
}

void SchedulerResourceReporter::FillPendingActorInfo(
    rpc::GetNodeStatsReply *reply) const {
  // Report infeasible actors.
  int num_reported = 0;
  for (const auto &shapes_it : infeasible_tasks_) {
    auto &work_queue = shapes_it.second;
    for (const auto &work_it : work_queue) {
      const RayTask &task = work_it->task;
      if (task.GetTaskSpecification().IsActorCreationTask()) {
        if (num_reported++ > kMaxPendingActorsToReport) {
          break;  // Protect the raylet from reporting too much data.
        }
        auto infeasible_task = reply->add_infeasible_tasks();
        infeasible_task->CopyFrom(task.GetTaskSpecification().GetMessage());
      }
    }
  }
  // Report actors blocked on resources.
  num_reported = 0;
  for (const auto &shapes_it :
       boost::range::join(tasks_to_dispatch_, tasks_to_schedule_)) {
    auto &work_queue = shapes_it.second;
    for (const auto &work_it : work_queue) {
      const RayTask &task = work_it->task;
      if (task.GetTaskSpecification().IsActorCreationTask()) {
        if (num_reported++ > kMaxPendingActorsToReport) {
          break;  // Protect the raylet from reporting too much data.
        }
        // TODO(scv119): we should report pending tasks instead.
        auto ready_task = reply->add_infeasible_tasks();
        ready_task->CopyFrom(task.GetTaskSpecification().GetMessage());
      }
    }
  }
}

void SchedulerResourceReporter::FillPendingActorCountByShape(
    rpc::ResourcesData &data) const {
  absl::flat_hash_map<SchedulingClass, std::pair<int, int>> pending_count_by_shape;
  for (const auto &[scheduling_class, queue] : infeasible_tasks_) {
    pending_count_by_shape[scheduling_class].first = queue.size();
  }
  for (const auto &[scheduling_class, queue] : tasks_to_schedule_) {
    pending_count_by_shape[scheduling_class].second = queue.size();
  }

  if (!pending_count_by_shape.empty()) {
    data.set_cluster_full_of_actors_detected(true);
    auto resource_load_by_shape =
        data.mutable_resource_load_by_shape()->mutable_resource_demands();
    for (const auto &shape_entry : pending_count_by_shape) {
      auto by_shape_entry = resource_load_by_shape->Add();
      for (const auto &resource_entry :
           TaskSpecification::GetSchedulingClassDescriptor(shape_entry.first)
               .resource_set.GetResourceMap()) {
        (*by_shape_entry->mutable_shape())[resource_entry.first] = resource_entry.second;
      }
      by_shape_entry->set_num_infeasible_requests_queued(shape_entry.second.first);
      by_shape_entry->set_num_ready_requests_queued(shape_entry.second.second);
    }
  }
}

}  // namespace raylet
}  // namespace ray
