#include "task.h"
#include "task_table.h"

#include "global_scheduler_algorithm.h"

int64_t local_scheduler_round_robin_index = 0;

void handle_task_waiting(global_scheduler_state *state, task *task) {
  if (utarray_len(state->local_schedulers) > 0) {
    local_scheduler *scheduler =
        (local_scheduler *) utarray_eltptr(state->local_schedulers,
                                           local_scheduler_round_robin_index);
    local_scheduler_round_robin_index += 1;
    local_scheduler_round_robin_index %= utarray_len(state->local_schedulers);
    assign_task_to_local_scheduler(state, task, scheduler->id);
  } else {
    CHECKM(0, "We currently don't handle this case.");
  }
}

void handle_object_available(global_scheduler_state *state,
                             object_id object_id) {
  /* Do nothing for now. */
}

void handle_local_scheduler_heartbeat(global_scheduler_state *state) {
  /* Do nothing for now. */
}

void handle_new_local_scheduler(global_scheduler_state *state,
                                db_client_id db_client_id) {
  local_scheduler local_scheduler;
  memset(&local_scheduler, 0, sizeof(local_scheduler));
  local_scheduler.id = db_client_id;
  utarray_push_back(state->local_schedulers, &local_scheduler);
}
