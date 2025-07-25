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

#include "ray/raylet/local_task_manager.h"

#include <google/protobuf/map.h>

#include <algorithm>
#include <boost/range/join.hpp>
#include <limits>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "ray/common/scheduling/cluster_resource_data.h"
#include "ray/stats/metric_defs.h"
#include "ray/util/logging.h"

namespace ray {
namespace raylet {

LocalTaskManager::LocalTaskManager(
    const NodeID &self_node_id,
    ClusterResourceScheduler &cluster_resource_scheduler,
    TaskDependencyManagerInterface &task_dependency_manager,
    internal::NodeInfoGetter get_node_info,
    WorkerPoolInterface &worker_pool,
    absl::flat_hash_map<WorkerID, std::shared_ptr<WorkerInterface>> &leased_workers,
    std::function<bool(const std::vector<ObjectID> &object_ids,
                       std::vector<std::unique_ptr<RayObject>> *results)>
        get_task_arguments,
    size_t max_pinned_task_arguments_bytes,
    std::function<int64_t(void)> get_time_ms,
    int64_t sched_cls_cap_interval_ms)
    : self_node_id_(self_node_id),
      self_scheduling_node_id_(self_node_id.Binary()),
      cluster_resource_scheduler_(cluster_resource_scheduler),
      task_dependency_manager_(task_dependency_manager),
      get_node_info_(get_node_info),
      max_resource_shapes_per_load_report_(
          RayConfig::instance().max_resource_shapes_per_load_report()),
      worker_pool_(worker_pool),
      leased_workers_(leased_workers),
      get_task_arguments_(get_task_arguments),
      max_pinned_task_arguments_bytes_(max_pinned_task_arguments_bytes),
      get_time_ms_(get_time_ms),
      sched_cls_cap_enabled_(RayConfig::instance().worker_cap_enabled()),
      sched_cls_cap_interval_ms_(sched_cls_cap_interval_ms),
      sched_cls_cap_max_ms_(RayConfig::instance().worker_cap_max_backoff_delay_ms()) {}

void LocalTaskManager::QueueAndScheduleTask(std::shared_ptr<internal::Work> work) {
  // If the local node is draining, the cluster task manager will
  // guarantee that the local node is not selected for scheduling.
  RAY_CHECK(!cluster_resource_scheduler_.GetLocalResourceManager().IsLocalNodeDraining());
  // The local node must be feasible if the cluster task manager decides to run the task
  // locally.
  RAY_CHECK(cluster_resource_scheduler_.GetClusterResourceManager().HasFeasibleResources(
      self_scheduling_node_id_,
      ResourceMapToResourceRequest(work->task.GetTaskSpecification()
                                       .GetRequiredPlacementResources()
                                       .GetResourceMap(),
                                   /*requires_object_store_memory=*/false)))
      << work->task.GetTaskSpecification().DebugString() << " "
      << cluster_resource_scheduler_.GetClusterResourceManager()
             .GetNodeResources(self_scheduling_node_id_)
             .DebugString();
  WaitForTaskArgsRequests(std::move(work));
  ScheduleAndDispatchTasks();
}

void LocalTaskManager::WaitForTaskArgsRequests(std::shared_ptr<internal::Work> work) {
  const auto &task = work->task;
  const auto &task_id = task.GetTaskSpecification().TaskId();
  const auto &scheduling_key = task.GetTaskSpecification().GetSchedulingClass();
  auto object_ids = task.GetTaskSpecification().GetDependencies();
  if (!object_ids.empty()) {
    bool args_ready = task_dependency_manager_.RequestTaskDependencies(
        task_id,
        task.GetDependencies(),
        {task.GetTaskSpecification().GetName(), task.GetTaskSpecification().IsRetry()});
    if (args_ready) {
      RAY_LOG(DEBUG) << "Args already ready, task can be dispatched " << task_id;
      tasks_to_dispatch_[scheduling_key].emplace_back(std::move(work));
    } else {
      RAY_LOG(DEBUG) << "Waiting for args for task: "
                     << task.GetTaskSpecification().TaskId();
      auto it = waiting_task_queue_.insert(waiting_task_queue_.end(), std::move(work));
      RAY_CHECK(waiting_tasks_index_.emplace(task_id, it).second);
    }
  } else {
    RAY_LOG(DEBUG) << "No args, task can be dispatched "
                   << task.GetTaskSpecification().TaskId();
    tasks_to_dispatch_[scheduling_key].emplace_back(std::move(work));
  }
}

void LocalTaskManager::ScheduleAndDispatchTasks() {
  DispatchScheduledTasksToWorkers();
  // TODO(swang): Spill from waiting queue first? Otherwise, we may end up
  // spilling a task whose args are already local.
  // TODO(swang): Invoke ScheduleAndDispatchTasks() when we run out of memory
  // in the PullManager or periodically, to make sure that we spill waiting
  // tasks that are blocked.
  SpillWaitingTasks();
}

void LocalTaskManager::DispatchScheduledTasksToWorkers() {
  // Check every task in task_to_dispatch queue to see
  // whether it can be dispatched and ran. This avoids head-of-line
  // blocking where a task which cannot be dispatched because
  // there are not enough available resources blocks other
  // tasks from being dispatched.
  for (auto shapes_it = tasks_to_dispatch_.begin();
       shapes_it != tasks_to_dispatch_.end();) {
    auto &scheduling_class = shapes_it->first;
    auto &dispatch_queue = shapes_it->second;

    auto sched_cls_iter = info_by_sched_cls_.find(scheduling_class);
    if (sched_cls_iter == info_by_sched_cls_.end()) {
      // Initialize the class info.
      sched_cls_iter = info_by_sched_cls_
                           .emplace(scheduling_class,
                                    SchedulingClassInfo(MaxRunningTasksPerSchedulingClass(
                                        scheduling_class)))
                           .first;
    }
    auto &sched_cls_info = sched_cls_iter->second;

    // Fair scheduling is applied only when the total CPU requests exceed the node's
    // capacity. This skips scheduling classes whose number of running tasks exceeds the
    // average number of tasks per scheduling class.

    // The purpose of fair scheduling is to ensure that each scheduling class has an
    // equal chance of being selected for dispatch. For instance, in a pipeline with both
    // data producers and consumers, we aim for consumers to have the same chance to be
    // dispatched as producers. This prevents memory peak caused by dispatching all
    // producer tasks first.
    // A scheduling class is skipped from dispatching if its number of running tasks
    // exceeds the fair_share, which is the average number of running tasks among all
    // scheduling classes. For example, consider a scenario where we have 3 CPUs and 2
    // scheduling classes, `f` and `g`, each with 4 tasks.
    // Status 1: The queue init with [f, f, f, f, g, g, g, g], and 0 running tasks.
    // Status 2: We dispatch 3 `f` tasks. Now the queue is [f, g, g, g, g],
    //           with 3 `f` tasks running.
    // Status 3: Suppose 1 `f` task finishes. When choosing the next task to dispatch,
    //           the queue is [f, g, g, g, g], and there are 2 `f` tasks running.
    //           We calculate fair_share as follows:
    //           fair_share = number of running tasks / number of scheduling classes
    //                       = 2 / 2 = 1.
    //           Since the number of running `f` tasks (2) is greater than the
    //           fair_share (1), we skip `f` and choose to dispatch `g`.
    // Note 1: Fair_share is calculated as (total number of running tasks with >0 CPU)
    //         / (number of scheduling classes in tasks_to_dispatch_).
    // Note 2: The decision to skip a scheduling class happens when loop through the
    //         scheduling classes (keys of tasks_to_dispatch_). This means we check for
    //         fair dispatching when looping through the scheduling classes rather than
    //         for each individual task, reducing the number of checks required.
    //         This is why in Status 2 of the example, we dispatch 3 `f` tasks because
    //         we chose `f` for dispatch,and we continue dispatching all `f`
    //         tasks until resources are fully utilized.

    // Currently, fair dispatching is implemented only for tasks that require CPU
    // resources. CPU. For details, see https://github.com/ray-project/ray/pull/44733.

    // Calculate the total CPU requests for all tasks in the tasks_to_dispatch queue.
    double total_cpu_requests_ = 0.0;

    // Count the number of scheduling classes that require CPU and sum their total CPU
    // requests.
    size_t num_classes_with_cpu = 0;
    for (const auto &[_, cur_dispatch_queue] : tasks_to_dispatch_) {
      // Only need to check the first because all tasks with the same scheduling class
      // have the same CPU resource requirements.
      RAY_CHECK(!cur_dispatch_queue.empty());
      const auto &work = cur_dispatch_queue.front();
      const auto &task_spec = work->task.GetTaskSpecification();
      auto cpu_request_ =
          task_spec.GetRequiredResources().Get(scheduling::ResourceID::CPU()).Double();
      if (cpu_request_ > 0) {
        num_classes_with_cpu++;
        total_cpu_requests_ += cur_dispatch_queue.size() * cpu_request_;
      }
    }
    const auto &sched_cls_desc =
        TaskSpecification::GetSchedulingClassDescriptor(scheduling_class);
    double total_cpus =
        cluster_resource_scheduler_.GetLocalResourceManager().GetNumCpus();

    // Compare total CPU requests with the node's total CPU capacity. If the requests
    // exceed the capacity, check if fair dispatching is needed.
    if (sched_cls_desc.resource_set.Get(scheduling::ResourceID::CPU()).Double() > 0 &&
        total_cpu_requests_ > total_cpus) {
      RAY_LOG(DEBUG)
          << "Applying fairness policy. Total CPU requests in tasks_to_dispatch_ ("
          << total_cpu_requests_ << ") exceed total CPUs available (" << total_cpus
          << ").";
      // Get the total number of running tasks requires CPU.
      size_t total_cpu_running_tasks = 0;
      for (auto &entry : info_by_sched_cls_) {
        // Only consider CPU requests
        const auto &cur_sched_cls_desc =
            TaskSpecification::GetSchedulingClassDescriptor(entry.first);
        if (cur_sched_cls_desc.resource_set.Get(scheduling::ResourceID::CPU()).Double() >
            0) {
          total_cpu_running_tasks += entry.second.running_tasks.size();
        }
      }

      // 1. We have confirmed that this is a scheduling class that requires CPU resources,
      //    hence num_classes_with_cpu >= 1 (cannot be 0) as this scheduling class is in
      //    tasks_to_dispatch_.
      // 2. We will compute fair_share as the ideal distribution of tasks among all
      //    scheduling classes in tasks_to_dispatch_. Then, we will check if the number of
      //    running tasks for this scheduling class exceeds its ideal fair_share.
      // 3. Note: We should get the num_classes_with_cpu from tasks_to_dispatch_
      //    instead of the info_by_sched_cls_ although total_cpu_running_tasks gets from
      //    the task running. First, info_by_sched_cls_ may not be initialized yet for
      //    some scheduling classes (as we initialize it in the loop). Second, we expect
      //    the number of running tasks for this scheduling class to not be much. However,
      //    if no tasks of this scheduling class are running, it will not be skipped.

      size_t fair_share = total_cpu_running_tasks / num_classes_with_cpu;
      if (sched_cls_info.running_tasks.size() > fair_share) {
        RAY_LOG(DEBUG) << "Skipping dispatch for scheduling class " << scheduling_class
                       << ". Running tasks (" << sched_cls_info.running_tasks.size()
                       << ") exceed fair share (" << fair_share << ").";
        shapes_it++;
        continue;
      }
    }

    /// We cap the maximum running tasks of a scheduling class to avoid
    /// scheduling too many tasks of a single type/depth, when there are
    /// deeper/other functions that should be run. We need to apply back
    /// pressure to limit the number of worker processes started in scenarios
    /// with nested tasks.
    bool is_infeasible = false;
    for (auto work_it = dispatch_queue.begin(); work_it != dispatch_queue.end();) {
      auto &work = *work_it;
      const auto &task = work->task;
      const auto &spec = task.GetTaskSpecification();
      TaskID task_id = spec.TaskId();
      if (work->GetState() == internal::WorkStatus::WAITING_FOR_WORKER) {
        work_it++;
        continue;
      }

      // Check if the scheduling class is at capacity now.
      if (sched_cls_cap_enabled_ &&
          sched_cls_info.running_tasks.size() >= sched_cls_info.capacity &&
          work->GetState() == internal::WorkStatus::WAITING) {
        RAY_LOG(DEBUG) << "Hit cap! time=" << get_time_ms_()
                       << " next update time=" << sched_cls_info.next_update_time;
        if (get_time_ms_() < sched_cls_info.next_update_time) {
          // We're over capacity and it's not time to admit a new task yet.
          // Calculate the next time we should admit a new task.
          int64_t current_capacity = sched_cls_info.running_tasks.size();
          int64_t allowed_capacity = sched_cls_info.capacity;
          int64_t exp = current_capacity - allowed_capacity;
          int64_t wait_time = sched_cls_cap_interval_ms_ * (1L << exp);
          if (wait_time > sched_cls_cap_max_ms_) {
            wait_time = sched_cls_cap_max_ms_;
            RAY_LOG(WARNING) << "Starting too many worker processes for a single type of "
                                "task. Worker process startup is being throttled.";
          }

          int64_t target_time = get_time_ms_() + wait_time;
          sched_cls_info.next_update_time =
              std::min(target_time, sched_cls_info.next_update_time);

          // While we're over capacity and cannot run the task,
          // try to spill to a node that can run it.
          bool did_spill = TrySpillback(work, is_infeasible);
          if (did_spill) {
            work_it = dispatch_queue.erase(work_it);
            continue;
          }

          break;
        }
      }

      bool args_missing = false;
      bool success = PinTaskArgsIfMemoryAvailable(spec, &args_missing);
      // An argument was evicted since this task was added to the dispatch
      // queue. Move it back to the waiting queue. The caller is responsible
      // for notifying us when the task is unblocked again.
      if (!success) {
        if (args_missing) {
          // Insert the task at the head of the waiting queue because we
          // prioritize spilling from the end of the queue.
          // TODO(scv119): where does pulling happen?
          auto it = waiting_task_queue_.insert(waiting_task_queue_.begin(),
                                               std::move(*work_it));
          RAY_CHECK(waiting_tasks_index_.emplace(task_id, it).second);
          work_it = dispatch_queue.erase(work_it);
        } else {
          // The task's args cannot be pinned due to lack of memory. We should
          // retry dispatching the task once another task finishes and releases
          // its arguments.
          RAY_LOG(DEBUG) << "Dispatching task " << task_id
                         << " would put this node over the max memory allowed for "
                            "arguments of executing tasks ("
                         << max_pinned_task_arguments_bytes_
                         << "). Waiting to dispatch task until other tasks complete";
          RAY_CHECK(!executing_task_args_.empty() && !pinned_task_arguments_.empty())
              << "Cannot dispatch task " << task_id
              << " until another task finishes and releases its arguments, but no other "
                 "task is running";
          work->SetStateWaiting(
              internal::UnscheduledWorkCause::WAITING_FOR_AVAILABLE_PLASMA_MEMORY);
          work_it++;
        }
        continue;
      }

      // Check if the node is still schedulable. It may not be if dependency resolution
      // took a long time.
      auto allocated_instances = std::make_shared<TaskResourceInstances>();
      bool schedulable =
          !cluster_resource_scheduler_.GetLocalResourceManager().IsLocalNodeDraining() &&
          cluster_resource_scheduler_.GetLocalResourceManager()
              .AllocateLocalTaskResources(spec.GetRequiredResources().GetResourceMap(),
                                          allocated_instances);
      if (!schedulable) {
        ReleaseTaskArgs(task_id);
        // The local node currently does not have the resources to run the task, so we
        // should try spilling to another node.
        bool did_spill = TrySpillback(work, is_infeasible);
        if (!did_spill) {
          // There must not be any other available nodes in the cluster, so the task
          // should stay on this node. We can skip the rest of the shape because the
          // scheduler will make the same decision.
          work->SetStateWaiting(
              internal::UnscheduledWorkCause::WAITING_FOR_RESOURCES_AVAILABLE);
          break;
        }
        work_it = dispatch_queue.erase(work_it);
      } else {
        // Force us to recalculate the next update time the next time a task
        // comes through this queue. We should only do this when we're
        // confident we're ready to dispatch the task after all checks have
        // passed.
        sched_cls_info.next_update_time = std::numeric_limits<int64_t>::max();
        sched_cls_info.running_tasks.insert(spec.TaskId());
        // The local node has the available resources to run the task, so we should run
        // it.
        work->allocated_instances = allocated_instances;
        work->SetStateWaitingForWorker();
        bool is_detached_actor = spec.IsDetachedActor();
        auto &owner_address = spec.CallerAddress();
        /// TODO(scv119): if a worker is not started, the resources is leaked and
        // task might be hanging.
        worker_pool_.PopWorker(
            spec,
            [this, task_id, scheduling_class, work, is_detached_actor, owner_address](
                const std::shared_ptr<WorkerInterface> worker,
                PopWorkerStatus status,
                const std::string &runtime_env_setup_error_message) -> bool {
              // TODO(hjiang): After getting the ready-to-use worker and task id, we're
              // able to get physical execution context.
              //
              // ownership chain: raylet has-a node manager, node manager has-a local task
              // manager.
              //
              // - PID: could get from available worker
              // - Attempt id: could pass a global attempt id generator from raylet
              // - Cgroup application folder: could pass from raylet

              return PoppedWorkerHandler(worker,
                                         status,
                                         task_id,
                                         scheduling_class,
                                         work,
                                         is_detached_actor,
                                         owner_address,
                                         runtime_env_setup_error_message);
            });
        work_it++;
      }
    }
    // In the beginning of the loop, we add scheduling_class
    // to the `info_by_sched_cls_` map.
    // In cases like dead owners, we may not add any tasks
    // to `running_tasks` so we can remove the map entry
    // for that scheduling_class to prevent memory leaks.
    if (sched_cls_info.running_tasks.size() == 0) {
      info_by_sched_cls_.erase(scheduling_class);
    }
    if (is_infeasible) {
      const auto &front_task = dispatch_queue.front()->task.GetTaskSpecification();
      RAY_LOG(ERROR) << "A task got scheduled to a node even though it was infeasible. "
                        "Please report an issue on GitHub.\nTask: "
                     << front_task.DebugString();
      auto dispatch_queue_iter = dispatch_queue.begin();
      while (dispatch_queue_iter != dispatch_queue.end()) {
        CancelTaskToDispatch(
            *dispatch_queue_iter,
            rpc::RequestWorkerLeaseReply::SCHEDULING_CANCELLED_UNSCHEDULABLE,
            "Scheduling failed due to the task becoming infeasible.");
        dispatch_queue_iter = dispatch_queue.erase(dispatch_queue_iter);
      }
      tasks_to_dispatch_.erase(shapes_it++);
    } else if (dispatch_queue.empty()) {
      tasks_to_dispatch_.erase(shapes_it++);
    } else {
      shapes_it++;
    }
  }
}

void LocalTaskManager::SpillWaitingTasks() {
  // Try to spill waiting tasks to a remote node, prioritizing those at the end
  // of the queue. Waiting tasks are spilled if there are enough remote
  // resources AND (we have no resources available locally OR their
  // dependencies are not being fetched). We should not spill tasks whose
  // dependencies are actively being fetched because some of their dependencies
  // may already be local or in-flight to this node.
  //
  // NOTE(swang): We do not iterate by scheduling class here, so if we break
  // due to lack of remote resources, it is possible that a waiting task that
  // is earlier in the queue could have been scheduled to a remote node.
  // TODO(scv119): this looks very aggressive: we will try to spillback
  // all the tasks in the waiting queue regardless of the wait time.
  auto it = waiting_task_queue_.end();
  while (it != waiting_task_queue_.begin()) {
    it--;
    const auto &task = (*it)->task;
    const auto &spec = task.GetTaskSpecification();
    const auto &task_id = spec.TaskId();

    // Check whether this task's dependencies are blocked (not being actively
    // pulled).  If this is true, then we should force the task onto a remote
    // feasible node, even if we have enough resources available locally for
    // placement.
    bool task_dependencies_blocked =
        task_dependency_manager_.TaskDependenciesBlocked(task_id);
    RAY_LOG(DEBUG) << "Attempting to spill back waiting task " << task_id
                   << " to remote node. Dependencies blocked? "
                   << task_dependencies_blocked;
    bool is_infeasible;
    // TODO(swang): The policy currently does not account for the amount of
    // object store memory availability. Ideally, we should pick the node with
    // the most memory availability.
    scheduling::NodeID scheduling_node_id;
    if (!spec.IsSpreadSchedulingStrategy()) {
      scheduling_node_id = cluster_resource_scheduler_.GetBestSchedulableNode(
          spec,
          /*preferred_node_id*/ self_node_id_.Binary(),
          /*exclude_local_node*/ task_dependencies_blocked,
          /*requires_object_store_memory*/ true,
          &is_infeasible);
    } else {
      // If scheduling strategy is spread, we prefer honoring spread decision
      // and waiting for task dependencies to be pulled
      // locally than spilling back and causing uneven spread.
      scheduling_node_id = self_scheduling_node_id_;
    }

    if (!scheduling_node_id.IsNil() && scheduling_node_id != self_scheduling_node_id_) {
      NodeID node_id = NodeID::FromBinary(scheduling_node_id.Binary());
      Spillback(node_id, *it);
      if (!spec.GetDependencies().empty()) {
        task_dependency_manager_.RemoveTaskDependencies(spec.TaskId());
      }
      num_waiting_task_spilled_++;
      waiting_tasks_index_.erase(task_id);
      it = waiting_task_queue_.erase(it);
    } else {
      if (scheduling_node_id.IsNil()) {
        RAY_LOG(DEBUG) << "RayTask " << task_id
                       << " has blocked dependencies, but no other node has resources, "
                          "keeping the task local";
      } else {
        RAY_LOG(DEBUG) << "Keeping waiting task " << task_id << " local";
      }
      // We should keep the task local. Note that an earlier task in the queue
      // may have different resource requirements and could actually be
      // scheduled on a remote node.
      break;
    }
  }
}

bool LocalTaskManager::TrySpillback(const std::shared_ptr<internal::Work> &work,
                                    bool &is_infeasible) {
  const auto &spec = work->task.GetTaskSpecification();
  auto scheduling_node_id = cluster_resource_scheduler_.GetBestSchedulableNode(
      spec,
      // We should prefer to stay local if possible
      // to avoid unnecessary spillback
      // since this node is already selected by the cluster scheduler.
      /*preferred_node_id=*/self_node_id_.Binary(),
      /*exclude_local_node=*/false,
      /*requires_object_store_memory=*/false,
      &is_infeasible);

  if (is_infeasible || scheduling_node_id.IsNil() ||
      scheduling_node_id == self_scheduling_node_id_) {
    return false;
  }

  NodeID node_id = NodeID::FromBinary(scheduling_node_id.Binary());
  Spillback(node_id, work);
  num_unschedulable_task_spilled_++;
  if (!spec.GetDependencies().empty()) {
    task_dependency_manager_.RemoveTaskDependencies(spec.TaskId());
  }
  return true;
}

bool LocalTaskManager::PoppedWorkerHandler(
    const std::shared_ptr<WorkerInterface> worker,
    PopWorkerStatus status,
    const TaskID &task_id,
    SchedulingClass scheduling_class,
    const std::shared_ptr<internal::Work> &work,
    bool is_detached_actor,
    const rpc::Address &owner_address,
    const std::string &runtime_env_setup_error_message) {
  const auto &reply = work->reply;
  const auto &callback = work->callback;
  const bool canceled = work->GetState() == internal::WorkStatus::CANCELLED;
  const auto &task = work->task;
  bool dispatched = false;

  if (!canceled) {
    const auto &required_resource =
        task.GetTaskSpecification().GetRequiredResources().GetResourceMap();
    for (auto &entry : required_resource) {
      // This is to make sure PG resource is not deleted during popping worker
      // unless the lease request is cancelled.
      RAY_CHECK(cluster_resource_scheduler_.GetLocalResourceManager().ResourcesExist(
          scheduling::ResourceID(entry.first)))
          << entry.first;
    }
  }

  // Erases the work from task_to_dispatch_ queue, also removes the task dependencies.
  //
  // IDEA(ryw): Make an RAII class to wrap the a shared_ptr<internal::Work> and
  // requests task dependency upon ctor, and remove task dependency upon dtor.
  // I tried this, it works, but we expose the map via GetTaskToDispatch() used in
  // scheduler_resource_reporter.cc. Maybe we can use `boost::any_range` to only expose
  // a view of the Work ptrs, but I got dependency issues
  // (can't include boost/range/any_range.hpp).
  auto erase_from_dispatch_queue_fn = [this](const std::shared_ptr<internal::Work> &work,
                                             const SchedulingClass &scheduling_class) {
    auto shapes_it = tasks_to_dispatch_.find(scheduling_class);
    RAY_CHECK(shapes_it != tasks_to_dispatch_.end());
    auto &dispatch_queue = shapes_it->second;
    bool erased = false;
    for (auto work_it = dispatch_queue.begin(); work_it != dispatch_queue.end();
         work_it++) {
      if (*work_it == work) {
        dispatch_queue.erase(work_it);
        erased = true;
        break;
      }
    }
    if (dispatch_queue.empty()) {
      tasks_to_dispatch_.erase(shapes_it);
    }
    RAY_CHECK(erased);

    const auto &task = work->task;
    if (!task.GetDependencies().empty()) {
      task_dependency_manager_.RemoveTaskDependencies(
          task.GetTaskSpecification().TaskId());
    }
  };

  if (canceled) {
    // Task has been canceled.
    RAY_LOG(DEBUG) << "Task " << task_id << " has been canceled when worker popped";
    RemoveFromRunningTasksIfExists(task);
    // All the cleaning work has been done when canceled task. Just return
    // false without doing anything.
    return false;
  }

  if (!worker) {
    dispatched = false;
    // We've already acquired resources so we need to release them.
    cluster_resource_scheduler_.GetLocalResourceManager().ReleaseWorkerResources(
        work->allocated_instances);
    work->allocated_instances = nullptr;
    // Release pinned task args.
    ReleaseTaskArgs(task_id);
    RemoveFromRunningTasksIfExists(task);

    // Empty worker popped.
    RAY_LOG(DEBUG).WithField(task_id)
        << "This node has available resources, but no worker processes "
           "to grant the lease: status "
        << status;
    if (status == PopWorkerStatus::RuntimeEnvCreationFailed ||
        status == PopWorkerStatus::ArgumentListTooLong) {
      // In case of runtime env creation or worker startup failure, we cancel this task
      // directly and raise an exception to user eventually. The task will be removed
      // from the dispatch queue in `CancelTask`.
      CancelTasks(
          [task_id](const auto &work) {
            return task_id == work->task.GetTaskSpecification().TaskId();
          },
          rpc::RequestWorkerLeaseReply::SCHEDULING_CANCELLED_RUNTIME_ENV_SETUP_FAILED,
          /*scheduling_failure_message*/ runtime_env_setup_error_message);
    } else if (status == PopWorkerStatus::JobFinished) {
      // The task job finished.
      // Just remove the task from dispatch queue.
      RAY_LOG(DEBUG) << "Call back to a job finished task, task id = " << task_id;
      erase_from_dispatch_queue_fn(work, scheduling_class);
    } else {
      // In other cases, set the work status `WAITING` to make this task
      // could be re-dispatched.
      internal::UnscheduledWorkCause cause =
          internal::UnscheduledWorkCause::WORKER_NOT_FOUND_JOB_CONFIG_NOT_EXIST;
      if (status == PopWorkerStatus::JobConfigMissing) {
        cause = internal::UnscheduledWorkCause::WORKER_NOT_FOUND_JOB_CONFIG_NOT_EXIST;
      } else if (status == PopWorkerStatus::WorkerPendingRegistration) {
        cause = internal::UnscheduledWorkCause::WORKER_NOT_FOUND_REGISTRATION_TIMEOUT;
      } else {
        RAY_LOG(FATAL) << "Unexpected state received for the empty pop worker. Status: "
                       << status;
      }
      work->SetStateWaiting(cause);
    }
  } else {
    // A worker has successfully popped for a valid task. Dispatch the task to
    // the worker.
    RAY_LOG(DEBUG) << "Dispatching task " << task_id << " to worker "
                   << worker->WorkerId();

    Dispatch(worker, leased_workers_, work->allocated_instances, task, reply, callback);
    erase_from_dispatch_queue_fn(work, scheduling_class);
    dispatched = true;
  }

  return dispatched;
}

void LocalTaskManager::Spillback(const NodeID &spillback_to,
                                 const std::shared_ptr<internal::Work> &work) {
  auto send_reply_callback = work->callback;

  if (work->grant_or_reject) {
    work->reply->set_rejected(true);
    send_reply_callback();
    return;
  }

  num_task_spilled_++;
  const auto &task = work->task;
  const auto &task_spec = task.GetTaskSpecification();
  RAY_LOG(DEBUG) << "Spilling task " << task_spec.TaskId() << " to node " << spillback_to;

  if (!cluster_resource_scheduler_.AllocateRemoteTaskResources(
          scheduling::NodeID(spillback_to.Binary()),
          task_spec.GetRequiredResources().GetResourceMap())) {
    RAY_LOG(DEBUG) << "Tried to allocate resources for request " << task_spec.TaskId()
                   << " on a remote node that are no longer available";
  }

  auto node_info_ptr = get_node_info_(spillback_to);
  RAY_CHECK(node_info_ptr)
      << "Spilling back to a node manager, but no GCS info found for node "
      << spillback_to;
  auto reply = work->reply;
  reply->mutable_retry_at_raylet_address()->set_ip_address(
      node_info_ptr->node_manager_address());
  reply->mutable_retry_at_raylet_address()->set_port(node_info_ptr->node_manager_port());
  reply->mutable_retry_at_raylet_address()->set_raylet_id(spillback_to.Binary());

  send_reply_callback();
}

void LocalTaskManager::TasksUnblocked(const std::vector<TaskID> &ready_ids) {
  if (ready_ids.empty()) {
    return;
  }

  for (const auto &task_id : ready_ids) {
    auto it = waiting_tasks_index_.find(task_id);
    if (it != waiting_tasks_index_.end()) {
      auto work = *it->second;
      const auto &task = work->task;
      const auto &scheduling_key = task.GetTaskSpecification().GetSchedulingClass();
      RAY_LOG(DEBUG) << "Args ready, task can be dispatched "
                     << task.GetTaskSpecification().TaskId();
      tasks_to_dispatch_[scheduling_key].push_back(work);
      waiting_task_queue_.erase(it->second);
      waiting_tasks_index_.erase(it);
    }
  }
  ScheduleAndDispatchTasks();
}

void LocalTaskManager::RemoveFromRunningTasksIfExists(const RayTask &task) {
  auto sched_cls = task.GetTaskSpecification().GetSchedulingClass();
  auto it = info_by_sched_cls_.find(sched_cls);
  if (it != info_by_sched_cls_.end()) {
    // TODO(hjiang): After remove the task id from `running_tasks`, corresponding cgroup
    // will be updated.
    it->second.running_tasks.erase(task.GetTaskSpecification().TaskId());
    if (it->second.running_tasks.size() == 0) {
      info_by_sched_cls_.erase(it);
    }
  }
}

void LocalTaskManager::TaskFinished(std::shared_ptr<WorkerInterface> worker,
                                    RayTask *task) {
  RAY_CHECK(worker != nullptr && task != nullptr);
  *task = worker->GetAssignedTask();
  RemoveFromRunningTasksIfExists(*task);

  ReleaseTaskArgs(task->GetTaskSpecification().TaskId());
  if (worker->GetAllocatedInstances() != nullptr) {
    ReleaseWorkerResources(worker);
  }
}

// TODO(scv119): task args related logic probaly belongs task dependency manager.
bool LocalTaskManager::PinTaskArgsIfMemoryAvailable(const TaskSpecification &spec,
                                                    bool *args_missing) {
  std::vector<std::unique_ptr<RayObject>> args;
  const auto &deps = spec.GetDependencyIds();
  if (!deps.empty()) {
    // This gets refs to the arguments stored in plasma. The refs should be
    // deleted once we no longer need to pin the arguments.
    if (!get_task_arguments_(deps, &args)) {
      *args_missing = true;
      return false;
    }
    for (size_t i = 0; i < deps.size(); i++) {
      if (args[i] == nullptr) {
        // This can happen if the task's arguments were all local at some
        // point, but then at least one was evicted before the task could
        // be dispatched to a worker.
        RAY_LOG(DEBUG)
            << "RayTask " << spec.TaskId() << " argument " << deps[i]
            << " was evicted before the task could be dispatched. This can happen "
               "when there are many objects needed on this node. The task will be "
               "scheduled once all of its dependencies are local.";
        *args_missing = true;
        return false;
      }
    }
  }

  *args_missing = false;
  size_t task_arg_bytes = 0;
  for (auto &arg : args) {
    task_arg_bytes += arg->GetSize();
  }
  RAY_LOG(DEBUG) << "RayTask " << spec.TaskId() << " has args of size " << task_arg_bytes;
  PinTaskArgs(spec, std::move(args));
  RAY_LOG(DEBUG) << "Size of pinned task args is now " << pinned_task_arguments_bytes_;
  if (max_pinned_task_arguments_bytes_ == 0) {
    // Max threshold for pinned args is not set.
    return true;
  }

  if (task_arg_bytes > max_pinned_task_arguments_bytes_) {
    RAY_LOG(WARNING)
        << "Dispatched task " << spec.TaskId() << " has arguments of size "
        << task_arg_bytes
        << ", but the max memory allowed for arguments of executing tasks is only "
        << max_pinned_task_arguments_bytes_;
  } else if (pinned_task_arguments_bytes_ > max_pinned_task_arguments_bytes_) {
    ReleaseTaskArgs(spec.TaskId());
    RAY_LOG(DEBUG) << "Cannot dispatch task " << spec.TaskId()
                   << " with arguments of size " << task_arg_bytes
                   << " current pinned bytes is " << pinned_task_arguments_bytes_;
    return false;
  }

  return true;
}

void LocalTaskManager::PinTaskArgs(const TaskSpecification &spec,
                                   std::vector<std::unique_ptr<RayObject>> args) {
  const auto &deps = spec.GetDependencyIds();
  // TODO(swang): This should really be an assertion, but we can sometimes
  // receive a duplicate task request if there is a failure and the original
  // version of the task has not yet been canceled.
  auto executed_task_inserted = executing_task_args_.emplace(spec.TaskId(), deps).second;
  if (executed_task_inserted) {
    for (size_t i = 0; i < deps.size(); i++) {
      auto [it, pinned_task_inserted] =
          pinned_task_arguments_.emplace(deps[i], std::make_pair(std::move(args[i]), 0));
      if (pinned_task_inserted) {
        // This is the first task that needed this argument.
        pinned_task_arguments_bytes_ += it->second.first->GetSize();
      }
      it->second.second++;
    }
  } else {
    RAY_LOG(DEBUG) << "Scheduler received duplicate task " << spec.TaskId()
                   << ", most likely because the first execution failed";
  }
}

void LocalTaskManager::ReleaseTaskArgs(const TaskID &task_id) {
  auto it = executing_task_args_.find(task_id);
  // TODO(swang): This should really be an assertion, but we can sometimes
  // receive a duplicate task request if there is a failure and the original
  // version of the task has not yet been canceled.
  if (it != executing_task_args_.end()) {
    for (auto &arg : it->second) {
      auto arg_it = pinned_task_arguments_.find(arg);
      RAY_CHECK(arg_it != pinned_task_arguments_.end());
      RAY_CHECK(arg_it->second.second > 0);
      arg_it->second.second--;
      if (arg_it->second.second == 0) {
        // This is the last task that needed this argument.
        pinned_task_arguments_bytes_ -= arg_it->second.first->GetSize();
        pinned_task_arguments_.erase(arg_it);
      }
    }
    executing_task_args_.erase(it);
  }
}

namespace {
void ReplyCancelled(const std::shared_ptr<internal::Work> &work,
                    rpc::RequestWorkerLeaseReply::SchedulingFailureType failure_type,
                    const std::string &scheduling_failure_message) {
  auto reply = work->reply;
  auto callback = work->callback;
  reply->set_canceled(true);
  reply->set_failure_type(failure_type);
  reply->set_scheduling_failure_message(scheduling_failure_message);
  callback();
}
}  // namespace

bool LocalTaskManager::CancelTasks(
    std::function<bool(const std::shared_ptr<internal::Work> &)> predicate,
    rpc::RequestWorkerLeaseReply::SchedulingFailureType failure_type,
    const std::string &scheduling_failure_message) {
  bool tasks_cancelled = false;

  ray::erase_if<SchedulingClass, std::shared_ptr<internal::Work>>(
      tasks_to_dispatch_, [&](const std::shared_ptr<internal::Work> &work) {
        if (!predicate(work)) {
          return false;
        }
        CancelTaskToDispatch(work, failure_type, scheduling_failure_message);
        tasks_cancelled = true;
        return true;
      });

  ray::erase_if<std::shared_ptr<internal::Work>>(
      waiting_task_queue_, [&](const std::shared_ptr<internal::Work> &work) {
        if (predicate(work)) {
          ReplyCancelled(work, failure_type, scheduling_failure_message);
          if (!work->task.GetTaskSpecification().GetDependencies().empty()) {
            task_dependency_manager_.RemoveTaskDependencies(
                work->task.GetTaskSpecification().TaskId());
          }
          waiting_tasks_index_.erase(work->task.GetTaskSpecification().TaskId());
          tasks_cancelled = true;
          return true;
        } else {
          return false;
        }
      });

  return tasks_cancelled;
}

void LocalTaskManager::CancelTaskToDispatch(
    const std::shared_ptr<internal::Work> &work,
    rpc::RequestWorkerLeaseReply::SchedulingFailureType failure_type,
    const std::string &scheduling_failure_message) {
  const TaskID task_id = work->task.GetTaskSpecification().TaskId();
  RAY_LOG(DEBUG) << "Canceling task " << task_id << " from dispatch queue.";
  ReplyCancelled(work, failure_type, scheduling_failure_message);
  if (work->GetState() == internal::WorkStatus::WAITING_FOR_WORKER) {
    // We've already acquired resources so we need to release them.
    cluster_resource_scheduler_.GetLocalResourceManager().ReleaseWorkerResources(
        work->allocated_instances);
    // Release pinned task args.
    ReleaseTaskArgs(task_id);
  }
  if (!work->task.GetTaskSpecification().GetDependencies().empty()) {
    task_dependency_manager_.RemoveTaskDependencies(
        work->task.GetTaskSpecification().TaskId());
  }
  RemoveFromRunningTasksIfExists(work->task);
  work->SetStateCancelled();
}

const RayTask *LocalTaskManager::AnyPendingTasksForResourceAcquisition(
    int *num_pending_actor_creation, int *num_pending_tasks) const {
  const RayTask *exemplar = nullptr;
  // We are guaranteed that these tasks are blocked waiting for resources after a
  // call to ScheduleAndDispatchTasks(). They may be waiting for workers as well, but
  // this should be a transient condition only.
  for (const auto &shapes_it : tasks_to_dispatch_) {
    auto &work_queue = shapes_it.second;
    for (const auto &work_it : work_queue) {
      const auto &work = *work_it;
      const auto &task = work_it->task;

      // If the work is not in the waiting state, it will be scheduled soon or won't be
      // scheduled. Consider as non-pending.
      if (work.GetState() != internal::WorkStatus::WAITING) {
        continue;
      }

      // If the work is not waiting for acquiring resources, we don't consider it as
      // there's resource deadlock.
      if (work.GetUnscheduledCause() !=
              internal::UnscheduledWorkCause::WAITING_FOR_RESOURCE_ACQUISITION &&
          work.GetUnscheduledCause() !=
              internal::UnscheduledWorkCause::WAITING_FOR_RESOURCES_AVAILABLE &&
          work.GetUnscheduledCause() !=
              internal::UnscheduledWorkCause::WAITING_FOR_AVAILABLE_PLASMA_MEMORY) {
        continue;
      }

      if (task.GetTaskSpecification().IsActorCreationTask()) {
        *num_pending_actor_creation += 1;
      } else {
        *num_pending_tasks += 1;
      }

      if (exemplar == nullptr) {
        exemplar = &task;
      }
    }
  }
  return exemplar;
}

void LocalTaskManager::Dispatch(
    std::shared_ptr<WorkerInterface> worker,
    absl::flat_hash_map<WorkerID, std::shared_ptr<WorkerInterface>> &leased_workers,
    const std::shared_ptr<TaskResourceInstances> &allocated_instances,
    const RayTask &task,
    rpc::RequestWorkerLeaseReply *reply,
    std::function<void(void)> send_reply_callback) {
  const auto &task_spec = task.GetTaskSpecification();

  if (task_spec.IsActorCreationTask()) {
    // The actor belongs to this worker now.
    worker->SetLifetimeAllocatedInstances(allocated_instances);
  } else {
    worker->SetAllocatedInstances(allocated_instances);
  }
  worker->SetAssignedTask(task);

  // Pass the contact info of the worker to use.
  reply->set_worker_pid(worker->GetProcess().GetId());
  reply->mutable_worker_address()->set_ip_address(worker->IpAddress());
  reply->mutable_worker_address()->set_port(worker->Port());
  reply->mutable_worker_address()->set_worker_id(worker->WorkerId().Binary());
  reply->mutable_worker_address()->set_raylet_id(self_node_id_.Binary());

  RAY_CHECK(leased_workers.find(worker->WorkerId()) == leased_workers.end());
  leased_workers[worker->WorkerId()] = worker;
  cluster_resource_scheduler_.GetLocalResourceManager().SetBusyFootprint(
      WorkFootprint::NODE_WORKERS);

  // Update our internal view of the cluster state.
  std::shared_ptr<TaskResourceInstances> allocated_resources;
  if (task_spec.IsActorCreationTask()) {
    allocated_resources = worker->GetLifetimeAllocatedInstances();
  } else {
    allocated_resources = worker->GetAllocatedInstances();
  }
  ::ray::rpc::ResourceMapEntry *resource;
  for (auto &resource_id : allocated_resources->ResourceIds()) {
    bool first = true;  // Set resource name only if at least one of its
                        // instances has available capacity.
    auto instances = allocated_resources->Get(resource_id);
    for (size_t inst_idx = 0; inst_idx < instances.size(); inst_idx++) {
      if (instances[inst_idx] > 0.) {
        if (first) {
          resource = reply->add_resource_mapping();
          resource->set_name(resource_id.Binary());
          first = false;
        }
        auto rid = resource->add_resource_ids();
        rid->set_index(inst_idx);
        rid->set_quantity(instances[inst_idx].Double());
      }
    }
  }
  // Send the result back.
  send_reply_callback();
}

void LocalTaskManager::ClearWorkerBacklog(const WorkerID &worker_id) {
  for (auto it = backlog_tracker_.begin(); it != backlog_tracker_.end();) {
    it->second.erase(worker_id);
    if (it->second.empty()) {
      backlog_tracker_.erase(it++);
    } else {
      ++it;
    }
  }
}

void LocalTaskManager::SetWorkerBacklog(SchedulingClass scheduling_class,
                                        const WorkerID &worker_id,
                                        int64_t backlog_size) {
  if (backlog_size == 0) {
    backlog_tracker_[scheduling_class].erase(worker_id);
    if (backlog_tracker_[scheduling_class].empty()) {
      backlog_tracker_.erase(scheduling_class);
    }
  } else {
    backlog_tracker_[scheduling_class][worker_id] = backlog_size;
  }
}

void LocalTaskManager::ReleaseWorkerResources(std::shared_ptr<WorkerInterface> worker) {
  RAY_CHECK(worker != nullptr);
  auto allocated_instances = worker->GetAllocatedInstances()
                                 ? worker->GetAllocatedInstances()
                                 : worker->GetLifetimeAllocatedInstances();
  if (allocated_instances == nullptr) {
    return;
  }

  if (worker->IsBlocked()) {
    // If the worker is blocked, its CPU instances have already been released. We clear
    // the CPU instances to avoid double freeing.

    // For PG, there may be two cpu resources: wildcard and indexed.
    std::vector<ResourceID> cpu_resource_ids;
    for (const auto &resource_id : allocated_instances->ResourceIds()) {
      if (IsCPUOrPlacementGroupCPUResource(resource_id)) {
        cpu_resource_ids.emplace_back(resource_id);
      }
    }

    for (const auto &cpu_resource_id : cpu_resource_ids) {
      allocated_instances->Remove(cpu_resource_id);
    }
  }

  cluster_resource_scheduler_.GetLocalResourceManager().ReleaseWorkerResources(
      allocated_instances);
  worker->ClearAllocatedInstances();
  worker->ClearLifetimeAllocatedInstances();
}

bool LocalTaskManager::ReleaseCpuResourcesFromBlockedWorker(
    std::shared_ptr<WorkerInterface> worker) {
  if (!worker || worker->IsBlocked()) {
    return false;
  }

  bool cpu_resources_released = false;
  if (worker->GetAllocatedInstances() != nullptr) {
    for (const auto &resource_id : worker->GetAllocatedInstances()->ResourceIds()) {
      if (IsCPUOrPlacementGroupCPUResource(resource_id)) {
        auto cpu_instances = worker->GetAllocatedInstances()->GetDouble(resource_id);
        cluster_resource_scheduler_.GetLocalResourceManager().AddResourceInstances(
            resource_id, cpu_instances);
        cpu_resources_released = true;

        // Cannot break since we need to release
        // both PG wildcard and indexed CPU resources.
      }
    }
  }

  if (cpu_resources_released) {
    worker->MarkBlocked();
    return true;
  } else {
    return false;
  }
}

bool LocalTaskManager::ReturnCpuResourcesToUnblockedWorker(
    std::shared_ptr<WorkerInterface> worker) {
  if (!worker || !worker->IsBlocked()) {
    return false;
  }

  bool cpu_resources_returned = false;
  if (worker->GetAllocatedInstances() != nullptr) {
    for (const auto &resource_id : worker->GetAllocatedInstances()->ResourceIds()) {
      if (IsCPUOrPlacementGroupCPUResource(resource_id)) {
        auto cpu_instances = worker->GetAllocatedInstances()->GetDouble(resource_id);
        // Important: we allow going negative here, since otherwise you can use infinite
        // CPU resources by repeatedly blocking / unblocking a task. By allowing it to go
        // negative, at most one task can "borrow" this worker's resources.
        cluster_resource_scheduler_.GetLocalResourceManager().SubtractResourceInstances(
            resource_id, cpu_instances, /*allow_going_negative=*/true);
        cpu_resources_returned = true;

        // Cannot break since we need to return
        // both PG wildcard and indexed CPU resources.
      }
    }
  }

  if (cpu_resources_returned) {
    worker->MarkUnblocked();
    return true;
  } else {
    return false;
  }
}

ResourceSet LocalTaskManager::CalcNormalTaskResources() const {
  ResourceSet total_normal_task_resources;
  for (auto &entry : leased_workers_) {
    std::shared_ptr<WorkerInterface> worker = entry.second;
    auto &task_spec = worker->GetAssignedTask().GetTaskSpecification();
    if (!task_spec.PlacementGroupBundleId().first.IsNil()) {
      continue;
    }

    auto task_id = worker->GetAssignedTaskId();
    auto actor_id = task_id.ActorId();
    if (!actor_id.IsNil() && task_id == TaskID::ForActorCreationTask(actor_id)) {
      // This task ID corresponds to an actor creation task.
      continue;
    }

    if (auto allocated_instances = worker->GetAllocatedInstances()) {
      auto resource_set = allocated_instances->ToResourceSet();
      // Blocked normal task workers have temporarily released its allocated CPU.
      if (worker->IsBlocked()) {
        for (const auto &resource_id : allocated_instances->ResourceIds()) {
          if (IsCPUOrPlacementGroupCPUResource(resource_id)) {
            resource_set.Set(resource_id, 0);
          }
        }
      }
      total_normal_task_resources += resource_set;
    }
  }
  return total_normal_task_resources;
}

uint64_t LocalTaskManager::MaxRunningTasksPerSchedulingClass(
    SchedulingClass sched_cls_id) const {
  auto sched_cls = TaskSpecification::GetSchedulingClassDescriptor(sched_cls_id);
  double cpu_req = sched_cls.resource_set.Get(ResourceID::CPU()).Double();
  uint64_t total_cpus =
      cluster_resource_scheduler_.GetLocalResourceManager().GetNumCpus();

  if (cpu_req == 0 || total_cpus == 0) {
    return std::numeric_limits<uint64_t>::max();
  }
  return static_cast<uint64_t>(std::round(total_cpus / cpu_req));
}

void LocalTaskManager::RecordMetrics() const {
  ray::stats::STATS_scheduler_tasks.Record(executing_task_args_.size(), "Executing");
  ray::stats::STATS_scheduler_tasks.Record(waiting_tasks_index_.size(), "Waiting");
}

void LocalTaskManager::DebugStr(std::stringstream &buffer) const {
  buffer << "Waiting tasks size: " << waiting_tasks_index_.size() << "\n";
  buffer << "Number of executing tasks: " << executing_task_args_.size() << "\n";
  buffer << "Number of pinned task arguments: " << pinned_task_arguments_.size() << "\n";
  buffer << "Number of total spilled tasks: " << num_task_spilled_ << "\n";
  buffer << "Number of spilled waiting tasks: " << num_waiting_task_spilled_ << "\n";
  buffer << "Number of spilled unschedulable tasks: " << num_unschedulable_task_spilled_
         << "\n";
  buffer << "Resource usage {\n";

  // Calculates how much resources are occupied by tasks or actors.
  // Only iterate upto this number to avoid excessive CPU usage.
  auto max_iteration = RayConfig::instance().worker_max_resource_analysis_iteration();
  uint32_t iteration = 0;
  for (const auto &worker : worker_pool_.GetAllRegisteredWorkers(
           /*filter_dead_workers*/ true)) {
    if (max_iteration < iteration++) {
      break;
    }
    if (worker->IsDead()        // worker is dead
        || worker->IsBlocked()  // worker is blocked by blocking Ray API
        || (worker->GetAssignedTaskId().IsNil() &&
            worker->GetActorId().IsNil())) {  // Tasks or actors not assigned
      // Then this shouldn't have allocated resources.
      continue;
    }

    const auto &task_or_actor_name = worker->GetAssignedTask()
                                         .GetTaskSpecification()
                                         .FunctionDescriptor()
                                         ->CallString();
    buffer << "    - (language="
           << rpc::Language_descriptor()->FindValueByNumber(worker->GetLanguage())->name()
           << " "
           << "actor_or_task=" << task_or_actor_name << " "
           << "pid=" << worker->GetProcess().GetId() << " "
           << "worker_id=" << worker->WorkerId() << "): "
           << worker->GetAssignedTask()
                  .GetTaskSpecification()
                  .GetRequiredResources()
                  .DebugString()
           << "\n";
  }
  buffer << "}\n";
  buffer << "Backlog Size per scheduling descriptor :{workerId: num backlogs}:\n";
  for (const auto &[sched_cls, worker_to_backlog_size] : backlog_tracker_) {
    const auto &descriptor = TaskSpecification::GetSchedulingClassDescriptor(sched_cls);
    buffer << "\t" << descriptor.ResourceSetStr() << ": {\n";
    for (const auto &[worker_id, backlog_size] : worker_to_backlog_size) {
      buffer << "\t\t" << worker_id << ": " << backlog_size << "\n";
    }
    buffer << "\t}\n";
  }
  buffer << "\n";
  buffer << "Running tasks by scheduling class:\n";

  for (const auto &pair : info_by_sched_cls_) {
    const auto &sched_cls = pair.first;
    const auto &info = pair.second;
    const auto &descriptor = TaskSpecification::GetSchedulingClassDescriptor(sched_cls);
    buffer << "    - " << descriptor.DebugString() << ": " << info.running_tasks.size()
           << "/" << info.capacity << "\n";
  }
}

}  // namespace raylet
}  // namespace ray
