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
#include <deque>
#include <utility>

#include "ray/common/ray_config.h"

namespace ray {
namespace raylet {

SchedulerResourceReporter::SchedulerResourceReporter(
    const absl::flat_hash_map<SchedulingClass,
                              std::deque<std::shared_ptr<internal::Work>>>
        &leases_to_schedule,
    const absl::flat_hash_map<SchedulingClass,
                              std::deque<std::shared_ptr<internal::Work>>>
        &infeasible_leases,
    const LocalLeaseManagerInterface &local_lease_manager)
    : max_resource_shapes_per_load_report_(
          RayConfig::instance().max_resource_shapes_per_load_report()),
      leases_to_schedule_(leases_to_schedule),
      leases_to_grant_(local_lease_manager.GetLeasesToGrant()),
      infeasible_leases_(infeasible_leases),
      backlog_tracker_(local_lease_manager.GetBackLogTracker()) {}

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

void SchedulerResourceReporter::FillResourceUsage(rpc::ResourcesData &data) const {
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
        // TODO(Alex): It's possible that we skip a different scheduling key which
        // contains the same resources.
        skipped_requests++;
        break;
      }

      const auto &scheduling_class_descriptor =
          SchedulingClassToIds::GetSchedulingClassDescriptor(scheduling_class);
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
      const auto &label_selectors = scheduling_class_descriptor.label_selector;
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

      // Add label selectors
      label_selectors.ToProto(by_shape_entry->add_label_selectors());

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
      leases_to_schedule_ | boost::adaptors::transformed(transform_func), false);
  auto leases_to_grant_range =
      leases_to_grant_ | boost::adaptors::transformed([](const auto &pair) {
        auto cnt = pair.second.size();
        // We should only report leases to be granted that do not have resources
        // allocated.
        for (const auto &lease : pair.second) {
          if (lease->allocated_instances_) {
            cnt--;
          }
        }
        return std::make_pair(pair.first, cnt);
      });
  fill_resource_usage_helper(leases_to_grant_range, false);

  fill_resource_usage_helper(
      infeasible_leases_ | boost::adaptors::transformed(transform_func), true);
  auto backlog_tracker_range = backlog_tracker_ |
                               boost::adaptors::transformed([](const auto &pair) {
                                 return std::make_pair(pair.first, 0);
                               }) |
                               boost::adaptors::filtered([&visited](const auto &pair) {
                                 return visited.count(pair.first) == 0;
                               });

  fill_resource_usage_helper(backlog_tracker_range, false);

  if (skipped_requests > 0) {
    RAY_LOG(WARNING) << "There are more than " << max_resource_shapes_per_load_report_
                     << " scheduling classes. Some resource loads may not be reported to "
                        "the autoscaler.";
  }
}

void SchedulerResourceReporter::FillPendingActorCountByShape(
    rpc::ResourcesData &data) const {
  absl::flat_hash_map<SchedulingClass, std::pair<int, int>> pending_count_by_shape;
  for (const auto &[scheduling_class, queue] : infeasible_leases_) {
    pending_count_by_shape[scheduling_class].first = queue.size();
  }
  for (const auto &[scheduling_class, queue] : leases_to_schedule_) {
    pending_count_by_shape[scheduling_class].second = queue.size();
  }

  if (!pending_count_by_shape.empty()) {
    data.set_cluster_full_of_actors_detected(true);
    auto resource_load_by_shape =
        data.mutable_resource_load_by_shape()->mutable_resource_demands();
    for (const auto &shape_entry : pending_count_by_shape) {
      auto by_shape_entry = resource_load_by_shape->Add();
      for (const auto &resource_entry :
           SchedulingClassToIds::GetSchedulingClassDescriptor(shape_entry.first)
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
