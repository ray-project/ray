#include <getopt.h>
#include <signal.h>
#include <stdlib.h>

#include "common.h"
#include "event_loop.h"
#include "global_scheduler.h"
#include "global_scheduler_algorithm.h"
#include "net.h"
#include "state/db_client_table.h"
#include "state/table.h"
#include "state/task_table.h"
#include "state/object_table.h"

/* This is used to define the array of local schedulers used to define the
 * global_scheduler_state type. */
UT_icd local_scheduler_icd = {sizeof(local_scheduler), NULL, NULL, NULL};

void assign_task_to_local_scheduler(global_scheduler_state *state,
                                    task *task,
                                    node_id node_id) {
  task_set_state(task, TASK_STATUS_SCHEDULED);
  task_set_node(task, node_id);
  retry_info retry = {
      .num_retries = 0, .timeout = 100, .fail_callback = NULL,
  };
  task_table_update(state->db, copy_task(task), &retry, NULL, NULL);
}

global_scheduler_state *init_global_scheduler(event_loop *loop,
                                              const char *redis_addr,
                                              int redis_port) {
  global_scheduler_state *state = malloc(sizeof(global_scheduler_state));
  state->db = db_connect(redis_addr, redis_port, "global_scheduler", "", -1);
  db_attach(state->db, loop, false);
  utarray_new(state->local_schedulers, &local_scheduler_icd);
  state->policy_state = init_global_scheduler_policy();
  return state;
}

void free_global_scheduler(global_scheduler_state *state) {
  db_disconnect(state->db);
  utarray_free(state->local_schedulers);
  destroy_global_scheduler_policy(state->policy_state);
  free(state);
}

/* We need this code so we can clean up when we get a SIGTERM signal. */

global_scheduler_state *g_state;

void signal_handler(int signal) {
  if (signal == SIGTERM) {
    free_global_scheduler(g_state);
    exit(0);
  }
}

/* End of the cleanup code. */

void process_task_waiting(task *task, void *user_context) {
  global_scheduler_state *state = (global_scheduler_state *) user_context;
  handle_task_waiting(state, state->policy_state, task);
}

void process_new_db_client(db_client_id db_client_id,
                           const char *client_type,
                           void *user_context) {
  global_scheduler_state *state = (global_scheduler_state *) user_context;
  if (strcmp(client_type, "photon") == 0) {
    handle_new_local_scheduler(state, state->policy_state, db_client_id);
  }
}

/**
 * Process notification about the new object location.
 *
 * @param object_id : id of the object with new location
 * @param manager_count: the count of new locations for this object
 * @param manager_vector: the vector with new Plasma Manager locations
 * @param user_context: user context passed to the object_table_subscribe()
 *
 * @return None
 */
void process_new_object_manager(object_id object_id, int manager_count,
    OWNER const char *manager_vector[], void *user_context) {
  global_scheduler_state *state = (global_scheduler_state *) user_context;

}

/* object info subscribe callback */
void process_new_object_info(object_id object_id, int64_t object_size,
    void *user_context) {
  global_scheduler_state *state = (global_scheduler_state *) user_context;
  /* TODO(atumanov): add object size information to object hash table in state */

}

void start_server(const char *redis_addr, int redis_port) {
  event_loop *loop = event_loop_create();
  g_state = init_global_scheduler(loop, redis_addr, redis_port);
  /* Generic retry information for notification subscriptions. */
  retry_info retry = {
      .num_retries = 0, .timeout = 100, .fail_callback = NULL,
  };
  /* TODO(rkn): subscribe to notifications from the object table. */
  /* Subscribe to notifications about new local schedulers. TODO(rkn): this
   * needs to also get all of the clients that registered with the database
   * before this call to subscribe. */
  db_client_table_subscribe(g_state->db, process_new_db_client,
                            (void *) g_state, &retry, NULL, NULL);
  /* Subscribe to notifications about waiting tasks. TODO(rkn): this may need to
   * get tasks that were submitted to the database before the subscribe. */
  task_table_subscribe(g_state->db, NIL_ID, TASK_STATUS_WAITING,
                       process_task_waiting, (void *) g_state, &retry, NULL,
                       NULL);

  object_table_subscribe(g_state->db, NIL_OBJECT_ID, process_new_object_manager,
      (void *) g_state, &retry, NULL, NULL);

  object_info_subscribe(g_state->db, process_new_object_info, (void *) g_state,
      &retry, NULL, NULL);

  /* Subscribe to notifications about new objects and object sizes. */
  /* Start the event loop. */
  event_loop_run(loop);
}

int main(int argc, char *argv[]) {
  signal(SIGTERM, signal_handler);
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
