#include <getopt.h>
#include <signal.h>
#include <stdlib.h>

#include "common.h"
#include "event_loop.h"
#include "global_scheduler.h"
#include "global_scheduler_algorithm.h"
#include "net.h"
#include "object_info.h"
#include "state/db_client_table.h"
#include "state/object_table.h"
#include "state/table.h"
#include "state/task_table.h"

/* This is used to define the array of local schedulers used to define the
 * global_scheduler_state type. */
UT_icd local_scheduler_icd = {sizeof(local_scheduler), NULL, NULL, NULL};

void assign_task_to_local_scheduler(global_scheduler_state *state,
                                    task *task,
                                    node_id node_id) {
  char id_string[ID_STRING_SIZE];
  LOG_DEBUG("assigning task to node_id = %s",
            object_id_to_string(node_id, id_string, ID_STRING_SIZE));
  task_set_state(task, TASK_STATUS_SCHEDULED);
  task_set_node(task, node_id);
  retry_info retry = {
      .num_retries = 0, .timeout = 100, .fail_callback = NULL,
  };
  LOG_DEBUG("Issuing a task table update for task = %s",
            object_id_to_string(task_task_id(task), id_string, ID_STRING_SIZE));
  UNUSED(id_string);
  task_table_update(state->db, copy_task(task), &retry, NULL, NULL);
}

global_scheduler_state *init_global_scheduler(event_loop *loop,
                                              const char *redis_addr,
                                              int redis_port) {
  global_scheduler_state *state = malloc(sizeof(global_scheduler_state));
  /* Must initialize state to 0. Sets hashmap head(s) to NULL. */
  memset(state, 0, sizeof(global_scheduler_state));
  state->db = db_connect(redis_addr, redis_port, "global_scheduler", "", -1);
  db_attach(state->db, loop, false);
  utarray_new(state->local_schedulers, &local_scheduler_icd);
  state->policy_state = init_global_scheduler_policy();
  return state;
}

void free_global_scheduler(global_scheduler_state *state) {
  aux_address_entry *entry, *tmp;

  db_disconnect(state->db);
  utarray_free(state->local_schedulers);
  destroy_global_scheduler_policy(state->policy_state);
  /* Delete the plasma 2 photon association map. */
  HASH_ITER(hh, state->plasma_photon_map, entry, tmp) {
    HASH_DELETE(hh, state->plasma_photon_map, entry);
    /* Now deallocate hash table entry. */
    free(entry->aux_address);
    free(entry);
  }
  /* Free the scheduler object info table. */
  scheduler_object_info *object_entry, *tmp_entry;
  HASH_ITER(hh, state->scheduler_object_info_table, object_entry, tmp_entry) {
    HASH_DELETE(hh, state->scheduler_object_info_table, object_entry);
    utarray_free(object_entry->object_locations);
    free(object_entry);
  }
  /* Free the global scheduler state. */
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
  LOG_DEBUG("Task waiting callback is called.");
  handle_task_waiting(state, state->policy_state, task);
}

/**
 * Process a notification about a new DB client connecting to Redis.
 * @param aux_address: an ip:port pair for the plasma manager associated with
 * this db client.
 */
void process_new_db_client(db_client_id db_client_id,
                           const char *client_type,
                           const char *aux_address,
                           void *user_context) {
  global_scheduler_state *state = (global_scheduler_state *) user_context;
  char id_string[ID_STRING_SIZE];
  LOG_DEBUG("db client table callback for db client = %s",
            object_id_to_string(db_client_id, id_string, ID_STRING_SIZE));
  UNUSED(id_string);
  if (strncmp(client_type, "photon", strlen("photon")) == 0) {
    /* Add plasma_manager ip:port -> photon_db_client_id association to state.
     */
    aux_address_entry *plasma_photon_entry =
        calloc(1, sizeof(aux_address_entry));
    plasma_photon_entry->aux_address = strdup(aux_address);
    plasma_photon_entry->photon_db_client_id = db_client_id;
    HASH_ADD_KEYPTR(
        hh, state->plasma_photon_map, plasma_photon_entry->aux_address,
        strlen(plasma_photon_entry->aux_address), plasma_photon_entry);

    {
      /* print the photon 2 plasma association map so far */
      aux_address_entry *entry, *tmp;
      LOG_DEBUG("[GS] P2P hash map so far:");
      HASH_ITER(hh, state->plasma_photon_map, entry, tmp) {
        LOG_DEBUG("%s -> %s", entry->aux_address,
                  object_id_to_string(entry->photon_db_client_id, id_string,
                                      ID_STRING_SIZE));
      }
    }

    /* add new local scheduler to the state. */
    handle_new_local_scheduler(state, state->policy_state, db_client_id);
  }
}

/**
 * Process notification about the new object information.
 *
 * @param object_id : id of the object with new location
 * @param data_size: object size
 * @param manager_count: the count of new locations for this object
 * @param manager_vector: the vector with new Plasma Manager locations
 * @param user_context: user context passed to the object_table_subscribe()
 * @return None
 */
void object_table_subscribe_callback(object_id object_id,
                                     int64_t data_size,
                                     int manager_count,
                                     const char *manager_vector[],
                                     void *user_context) {
  /* Extract global scheduler state from the callback context. */
  global_scheduler_state *state = (global_scheduler_state *) user_context;
  char id_string[ID_STRING_SIZE];
  LOG_DEBUG("object table subscribe callback for OBJECT = %s",
            object_id_to_string(object_id, id_string, ID_STRING_SIZE));
  UNUSED(id_string);
  LOG_DEBUG("\tManagers<%d>:", manager_count);
  for (int i = 0; i < manager_count; i++) {
    LOG_DEBUG("\t\t%s", manager_vector[i]);
  }
  scheduler_object_info *obj_info_entry = NULL;

  HASH_FIND(hh, state->scheduler_object_info_table, &object_id,
            sizeof(object_id), obj_info_entry);

  if (obj_info_entry == NULL) {
    /* Construct a new object info hash table entry. */
    obj_info_entry = malloc(sizeof(scheduler_object_info));
    memset(obj_info_entry, 0, sizeof(scheduler_object_info));

    obj_info_entry->object_id = object_id;
    obj_info_entry->data_size = data_size;

    HASH_ADD(hh, state->scheduler_object_info_table, object_id,
             sizeof(obj_info_entry->object_id), obj_info_entry);
    LOG_DEBUG("New object added to object_info_table with id = %s",
              object_id_to_string(object_id, id_string, ID_STRING_SIZE));
    LOG_DEBUG("\tmanager locations:");
    for (int i = 0; i < manager_count; i++) {
      LOG_DEBUG("\t\t%s", manager_vector[i]);
    }
  }

  /* In all cases, replace the object location vector on each callback. */
  if (obj_info_entry->object_locations != NULL) {
    utarray_free(obj_info_entry->object_locations);
    obj_info_entry->object_locations = NULL;
  }

  utarray_new(obj_info_entry->object_locations, &ut_str_icd);
  for (int i = 0; i < manager_count; i++) {
    utarray_push_back(obj_info_entry->object_locations, &manager_vector[i]);
  }
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

  object_table_subscribe_to_notifications(g_state->db, true,
                                          object_table_subscribe_callback,
                                          g_state, &retry, NULL, NULL);
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
