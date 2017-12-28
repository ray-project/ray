#include <limits.h>

#include "task.h"
#include "state/task_table.h"

#include "global_scheduler_algorithm.h"

GlobalSchedulerPolicyState *GlobalSchedulerPolicyState_init(void) {
  GlobalSchedulerPolicyState *policy_state = new GlobalSchedulerPolicyState();
  policy_state->round_robin_index = 0;
  return policy_state;
}

void GlobalSchedulerPolicyState_free(GlobalSchedulerPolicyState *policy_state) {
  delete policy_state;
}

/**
 * Checks if the given local scheduler satisfies the task's hard constraints.
 *
 * @param scheduler Local scheduler.
 * @param spec Task specification.
 * @return True if all tasks's resource constraints are satisfied. False
 *         otherwise.
 */
bool constraints_satisfied_hard(const LocalScheduler *scheduler,
                                const TaskSpec *spec) {
  for (auto const &resource_pair : TaskSpec_get_required_resources(spec)) {
    std::string resource_name = resource_pair.first;
    double resource_quantity = resource_pair.second;

    // Continue on if the task doesn't actually require this resource.
    if (resource_quantity == 0) {
      continue;
    }

    // Check if the local scheduler has this resource.
    if (scheduler->info.static_resources.count(resource_name) == 0) {
      return false;
    }

    // Check if the local scheduler has enough of the resource.
    if (scheduler->info.static_resources.at(resource_name) <
        resource_quantity) {
      return false;
    }
  }
  return true;
}

int64_t locally_available_data_size(const GlobalSchedulerState *state,
                                    DBClientID local_scheduler_id,
                                    TaskSpec *task_spec) {
  /* This function will compute the total size of all the object dependencies
   * for the given task that are already locally available to the specified
   * local scheduler. */
  int64_t task_data_size = 0;

  CHECK(state->local_scheduler_plasma_map.count(local_scheduler_id) == 1);

  const std::string &plasma_manager =
      state->local_scheduler_plasma_map.at(local_scheduler_id);

  /* TODO(rkn): Note that if the same object ID appears as multiple arguments,
   * then it will be overcounted. */
  for (int64_t i = 0; i < TaskSpec_num_args(task_spec); ++i) {
    int count = TaskSpec_arg_id_count(task_spec, i);
    for (int j = 0; j < count; ++j) {
      ObjectID object_id = TaskSpec_arg_id(task_spec, i, j);

      if (state->scheduler_object_info_table.count(object_id) == 0) {
        /* If this global scheduler is not aware of this object ID, then ignore
         * it. */
        continue;
      }

      const SchedulerObjectInfo &object_size_info =
          state->scheduler_object_info_table.at(object_id);

      if (std::find(object_size_info.object_locations.begin(),
                    object_size_info.object_locations.end(), plasma_manager) ==
          object_size_info.object_locations.end()) {
        /* This local scheduler does not have access to this object, so don't
         * count this object. */
        continue;
      }

      /* Look at the size of the object. */
      int64_t object_size = object_size_info.data_size;
      if (object_size == -1) {
        /* This means that this global scheduler does not know the object size
         * yet, so assume that the object is one megabyte. TODO(rkn): Maybe we
         * should instead use the average object size. */
        object_size = 1000000;
      }

      /* If we get here, then this local scheduler has access to this object, so
       * count the contribution of this object. */
      task_data_size += object_size;
    }
  }

  return task_data_size;
}

double calculate_cost_pending(const GlobalSchedulerState *state,
                              const LocalScheduler *scheduler,
                              TaskSpec *task_spec) {
  /* Calculate how much data is already present on this machine. TODO(rkn): Note
   * that this information is not being used yet. Fix this. */
  locally_available_data_size(state, scheduler->id, task_spec);
  /* TODO(rkn): This logic does not load balance properly when the different
   * machines have different sizes. Fix this. */
  return scheduler->num_recent_tasks_sent + scheduler->info.task_queue_length;
}

bool handle_task_waiting(GlobalSchedulerState *state,
                         GlobalSchedulerPolicyState *policy_state,
                         Task *task) {
  TaskSpec *task_spec = Task_task_execution_spec(task)->Spec();

  CHECKM(task_spec != NULL,
         "task wait handler encounted a task with NULL spec");

  bool task_feasible = false;

  /* Go through all the nodes, calculate the score for each, pick max score. */
  double best_local_scheduler_score = INT32_MIN;
  CHECKM(best_local_scheduler_score < 0,
         "We might have a floating point underflow");
  DBClientID best_local_scheduler_id =
      DBClientID::nil(); /* best node to send this task */
  for (auto it = state->local_schedulers.begin();
       it != state->local_schedulers.end(); it++) {
    /* For each local scheduler, calculate its score. Check hard constraints
     * first. */
    LocalScheduler *scheduler = &(it->second);
    if (!constraints_satisfied_hard(scheduler, task_spec)) {
      continue;
    }
    task_feasible = true;
    /* This node satisfies the hard capacity constraint. Calculate its score. */
    double score = -1 * calculate_cost_pending(state, scheduler, task_spec);
    if (score > best_local_scheduler_score) {
      best_local_scheduler_score = score;
      best_local_scheduler_id = scheduler->id;
    }
  }

  if (!task_feasible) {
    std::string id_string = Task_task_id(task).hex();
    LOG_ERROR(
        "Infeasible task. No nodes satisfy hard constraints for task = %s",
        id_string.c_str());
    /* TODO(atumanov): propagate this error to the task's driver and/or
     * cache the task in case new local schedulers satisfy it in the future. */
    return false;
  }
  CHECKM(!best_local_scheduler_id.is_nil(),
         "Task is feasible, but doesn't have a local scheduler assigned.");
  /* A local scheduler ID was found, so assign the task. */
  assign_task_to_local_scheduler(state, task, best_local_scheduler_id);
  return true;
}

void handle_object_available(GlobalSchedulerState *state,
                             GlobalSchedulerPolicyState *policy_state,
                             ObjectID object_id) {
  /* Do nothing for now. */
}

void handle_new_local_scheduler(GlobalSchedulerState *state,
                                GlobalSchedulerPolicyState *policy_state,
                                DBClientID db_client_id) {
  /* Do nothing for now. */
}

void handle_local_scheduler_removed(GlobalSchedulerState *state,
                                    GlobalSchedulerPolicyState *policy_state,
                                    DBClientID db_client_id) {
  /* Do nothing for now. */
}
