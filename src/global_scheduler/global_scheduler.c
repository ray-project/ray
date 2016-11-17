#include <getopt.h>
#include <stdlib.h>

#include "state/db.h"

#include "common.h"
#include "event_loop.h"
#include "global_scheduler_algorithm.h"
#include "global_scheduler_shared.h"
#include "local_scheduler_table.h"
#include "net.h"
#include "table.h"
#include "task_table.h"

/* This is used to define the array of local schedulers used to define the
 * global_scheduler_state type. */
UT_icd local_scheduler_icd = {sizeof(local_scheduler), NULL, NULL, NULL};

void assign_task_to_local_scheduler(global_scheduler_state *state,
                                    task *original_task,
                                    node_id node_id) {
  task *updated_task =
      alloc_task(task_task_spec(original_task), TASK_STATUS_SCHEDULED, node_id);
  retry_info retry = {
      .num_retries = 0, .timeout = 100, .fail_callback = NULL,
  };
  task_table_update(state->db, updated_task, &retry, NULL, NULL);
  free_task(updated_task);
}

global_scheduler_state *init_global_scheduler(event_loop *loop,
                                              const char *redis_addr,
                                              int redis_port) {
  global_scheduler_state *state = malloc(sizeof(global_scheduler_state));
  state->db = db_connect(redis_addr, redis_port, "global_scheduler", "", -1);
  db_attach(state->db, loop);
  utarray_new(state->local_schedulers, &local_scheduler_icd);
  return state;
}

void start_server(const char* redis_addr, int redis_port) {
  event_loop *loop = event_loop_create();
  global_scheduler_state *state =
      init_global_scheduler(loop, redis_addr, redis_port);
  /* Generic retry information for notification subscriptions. */
  retry_info retry = {
      .num_retries = 0, .timeout = 100, .fail_callback = NULL,
  };
  /* TODO(rkn): subscribe to notifications from the object table. */
  /* Subscribe to notifications about new local schedulers. */
  local_scheduler_table_subscribe(state->db, handle_new_local_scheduler,
                                  (void *) state, &retry, NULL, NULL);
  /* Subscribe to notifications about waiting tasks. */
  task_table_subscribe(state->db, NIL_ID, TASK_STATUS_WAITING,
                       handle_task_waiting, (void *) state, &retry, NULL, NULL);
  /* Start the event loop. */
  event_loop_run(loop);
}

int main(int argc, char *argv[]) {
  /* IP address and port of redis. */
  char *redis_addr_port = NULL;
  int c;
  while ((c = getopt(argc, argv, "s:m:h:p:r:")) != -1) {
    switch (c) {
    case 'r':
      redis_addr_port = optarg;
      break;
    default:
      LOG_ERROR("unknown option %c", c);
      exit(-1);
    }
  }
  char redis_addr[16];
  int redis_port;
  if (!redis_addr_port ||
      parse_ip_addr_port(redis_addr_port, redis_addr, &redis_port) == -1) {
    LOG_ERROR(
        "need to specify redis address like 127.0.0.1:6379 with -r switch");
    exit(-1);
  }
  start_server(redis_addr, redis_port);
}
