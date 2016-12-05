#include "task.h"
#include "task_table.h"

#include "global_scheduler_algorithm.h"

global_scheduler_policy_state *init_global_scheduler_policy(void) {
  global_scheduler_policy_state *policy_state =
      malloc(sizeof(global_scheduler_policy_state));
  policy_state->round_robin_index = 0;
  return policy_state;
}

void destroy_global_scheduler_policy(
    global_scheduler_policy_state *policy_state) {
  free(policy_state);
}

void handle_task_waiting(global_scheduler_state *state,
                         global_scheduler_policy_state *policy_state,
                         task *task) {
  if (utarray_len(state->local_schedulers) > 0) {
    local_scheduler *scheduler = (local_scheduler *) utarray_eltptr(
        state->local_schedulers, policy_state->round_robin_index);
    policy_state->round_robin_index += 1;
    policy_state->round_robin_index %= utarray_len(state->local_schedulers);
    assign_task_to_local_scheduler(state, task, scheduler->id);
  } else {
    CHECKM(0, "We currently don't handle this case.");
  }
}

void handle_object_available(global_scheduler_state *state,
                             global_scheduler_policy_state *policy_state,
                             object_id object_id) {
  /* Do nothing for now. */
}

void handle_local_scheduler_heartbeat(
    global_scheduler_state *state,
    global_scheduler_policy_state *policy_state) {
  /* Do nothing for now. */
}

void handle_new_local_scheduler(global_scheduler_state *state,
                                global_scheduler_policy_state *policy_state,
                                db_client_id db_client_id) {
  local_scheduler local_scheduler;
  memset(&local_scheduler, 0, sizeof(local_scheduler));
  local_scheduler.id = db_client_id;
  utarray_push_back(state->local_schedulers, &local_scheduler);
}
