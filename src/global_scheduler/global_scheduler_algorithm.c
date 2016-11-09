#include "task.h"
#include "task_table.h"

#include "global_scheduler_algorithm.h"

void handle_task_waiting(task *original_task, void *user_context) {
  global_scheduler_state *state = (global_scheduler_state *) user_context;
  if (utarray_len(state->local_schedulers) > 0) {
    local_scheduler *scheduler =
        (local_scheduler *) utarray_eltptr(state->local_schedulers, 0);
    assign_task_to_local_scheduler(state, original_task, scheduler->id);
  }
}

void handle_object_available(object_id obj_id) {
  /* Do nothing for now. */
}

void handle_object_unavailable(object_id obj_id) {
  /* Do nothing for now. */
}

void handle_local_scheduler_heartbeat(void) {
  /* Do nothing for now. */
}

void handle_new_local_scheduler(client_id client_id, void *user_context) {
  global_scheduler_state *state = (global_scheduler_state *) user_context;
  local_scheduler local_scheduler = {.id = client_id};
  utarray_push_back(state->local_schedulers, &local_scheduler);
}
