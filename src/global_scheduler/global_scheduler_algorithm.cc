#include <limits.h>

#include "task.h"
#include "state/task_table.h"

#include "global_scheduler_algorithm.h"

GlobalSchedulerPolicyState *GlobalSchedulerPolicyState_init(void) {
  GlobalSchedulerPolicyState *policy_state = new GlobalSchedulerPolicyState();
  policy_state->round_robin_index = 0;

  int num_weight_elem =
      sizeof(policy_state->resource_attribute_weight) / sizeof(double);
  for (int i = 0; i < num_weight_elem; i++) {
    /* Weight distribution is subject to scheduling policy. Giving all weight
     * to the last element of the vector (cached data) is equivalent to
     * the transfer-aware policy. */
    policy_state->resource_attribute_weight[i] = 1.0 / num_weight_elem;
  }

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
  for (int i = 0; i < ResourceIndex_MAX; i++) {
    if (scheduler->info.static_resources[i] <
        TaskSpec_get_required_resource(spec, i)) {
      return false;
    }
  }
  return true;
}

double inner_product(double a[], double b[], int size) {
  double result = 0;
  for (int i = 0; i < size; i++) {
    result += a[i] * b[i];
  }
  return result;
}

double calculate_score_dynvec_normalized(GlobalSchedulerState *state,
                                         LocalScheduler *scheduler,
                                         const TaskSpec *task_spec,
                                         double object_size_fraction) {
  /* The object size fraction is now calculated for this (task,node) pair. */
  /* Construct the normalized dynamic resource attribute vector */
  double normalized_dynvec[ResourceIndex_MAX + 1];
  memset(&normalized_dynvec, 0, sizeof(normalized_dynvec));
  for (int i = 0; i < ResourceIndex_MAX; i++) {
    double resreqval = TaskSpec_get_required_resource(task_spec, i);
    if (resreqval <= 0) {
      /* Skip and leave normalized dynvec value == 0. */
      continue;
    }
    normalized_dynvec[i] =
        MIN(1, scheduler->info.dynamic_resources[i] / resreqval);
  }
  normalized_dynvec[ResourceIndex_MAX] = object_size_fraction;

  /* Finally, calculate the score. */
  double score = inner_product(normalized_dynvec,
                               state->policy_state->resource_attribute_weight,
                               ResourceIndex_MAX + 1);
  return score;
}

double calculate_cost_pending(const GlobalSchedulerState *state,
                              const LocalScheduler *scheduler) {
  /* TODO: make sure that num_recent_tasks_sent is reset on each heartbeat. */
  return scheduler->num_recent_tasks_sent + scheduler->info.task_queue_length;
}

bool handle_task_waiting(GlobalSchedulerState *state,
                         GlobalSchedulerPolicyState *policy_state,
                         Task *task) {
  TaskSpec *task_spec = Task_task_spec(task);

  CHECKM(task_spec != NULL,
         "task wait handler encounted a task with NULL spec");

  bool task_feasible = false;
  /* The total size of the task's data. */
  int64_t task_object_size = 0;

  /* Go through all the nodes, calculate the score for each, pick max score. */
  LocalScheduler *scheduler = NULL;
  double best_local_scheduler_score = INT32_MIN;
  CHECKM(best_local_scheduler_score < 0,
         "We might have a floating point underflow");
  DBClientID best_local_scheduler_id = NIL_ID; /* best node to send this task */
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
    double score = -1 * calculate_cost_pending(state, scheduler);
    if (score > best_local_scheduler_score) {
      best_local_scheduler_score = score;
      best_local_scheduler_id = scheduler->id;
    }
  }

  if (!task_feasible) {
    char id_string[ID_STRING_SIZE];
    LOG_ERROR(
        "Infeasible task. No nodes satisfy hard constraints for task = %s",
        ObjectID_to_string(Task_task_id(task), id_string, ID_STRING_SIZE));
    /* TODO(atumanov): propagate this error to the task's driver and/or
     * cache the task in case new local schedulers satisfy it in the future. */
    return false;
  }
  CHECKM(!IS_NIL_ID(best_local_scheduler_id),
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
