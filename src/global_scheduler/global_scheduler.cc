#include <getopt.h>
#include <signal.h>
#include <stdlib.h>

#include "common.h"
#include "event_loop.h"
#include "global_scheduler.h"
#include "global_scheduler_algorithm.h"
#include "net.h"
#include "state/db_client_table.h"
#include "state/local_scheduler_table.h"
#include "state/object_table.h"
#include "state/table.h"
#include "state/task_table.h"

/* This is used to define the array of local schedulers used to define the
 * GlobalSchedulerState type. */
UT_icd local_scheduler_icd = {sizeof(LocalScheduler), NULL, NULL, NULL};

/* This is used to define the array of tasks that haven't been scheduled yet. */
UT_icd pending_tasks_icd = {sizeof(Task *), NULL, NULL, NULL};

/**
 * Assign the given task to the local scheduler, update Redis and scheduler data
 * structures.
 *
 * @param state Global scheduler state.
 * @param task Task to be assigned to the local scheduler.
 * @param local_scheduler_id DB client ID for the local scheduler.
 * @return Void.
 */
void assign_task_to_local_scheduler(GlobalSchedulerState *state,
                                    Task *task,
                                    DBClientID local_scheduler_id) {
  char id_string[ID_STRING_SIZE];
  TaskSpec *spec = Task_task_spec(task);
  LOG_DEBUG("assigning task to local_scheduler_id = %s",
            ObjectID_to_string(local_scheduler_id, id_string, ID_STRING_SIZE));
  Task_set_state(task, TASK_STATUS_SCHEDULED);
  Task_set_local_scheduler(task, local_scheduler_id);
  LOG_DEBUG("Issuing a task table update for task = %s",
            ObjectID_to_string(Task_task_id(task), id_string, ID_STRING_SIZE));
  UNUSED(id_string);
  task_table_update(state->db, Task_copy(task), NULL, NULL, NULL);

  /* TODO(rkn): We should probably pass around local_scheduler struct pointers
   * instead of db_client_id objects. */
  /* Update the local scheduler info. */
  LocalScheduler *local_scheduler =
      get_local_scheduler(state, local_scheduler_id);
  local_scheduler->num_tasks_sent += 1;
  local_scheduler->num_recent_tasks_sent += 1;
  /* Resource accounting update for this local scheduler. */
  for (int i = 0; i < ResourceIndex_MAX; i++) {
    /* Subtract task's resource from the cached dynamic resource capacity for
     *  this local scheduler. This will be overwritten on the next heartbeat. */
    local_scheduler->info.dynamic_resources[i] =
        MAX(0, local_scheduler->info.dynamic_resources[i] -
                   TaskSpec_get_required_resource(spec, i));
  }
}

GlobalSchedulerState *GlobalSchedulerState_init(event_loop *loop,
                                                const char *node_ip_address,
                                                const char *redis_primary_addr,
                                                int redis_primary_port,
                                                const std::vector<std::string>& redis_shards_addrs,
                                                const std::vector<int> &redis_shards_ports) {
  GlobalSchedulerState *state =
      (GlobalSchedulerState *) malloc(sizeof(GlobalSchedulerState));
  /* Must initialize state to 0. Sets hashmap head(s) to NULL. */
  memset(state, 0, sizeof(GlobalSchedulerState));
  state->db = db_connect(std::string(redis_primary_addr), redis_primary_port,
                         redis_shards_addrs, redis_shards_ports,
                         "global_scheduler",
                         node_ip_address, 0, NULL);
  db_attach(state->db, loop, false);
  utarray_new(state->local_schedulers, &local_scheduler_icd);
  state->policy_state = GlobalSchedulerPolicyState_init();
  /* Initialize the array of tasks that have not been scheduled yet. */
  utarray_new(state->pending_tasks, &pending_tasks_icd);
  return state;
}

void GlobalSchedulerState_free(GlobalSchedulerState *state) {
  AuxAddressEntry *entry, *tmp;

  db_disconnect(state->db);
  utarray_free(state->local_schedulers);
  GlobalSchedulerPolicyState_free(state->policy_state);
  /* Delete the plasma to local scheduler association map. */
  HASH_ITER(plasma_local_scheduler_hh, state->plasma_local_scheduler_map, entry,
            tmp) {
    HASH_DELETE(plasma_local_scheduler_hh, state->plasma_local_scheduler_map,
                entry);
    /* The hash entry is shared with the local_scheduler_plasma hashmap and will
     * be freed there. */
    free(entry->aux_address);
  }

  /* Delete the local scheduler to plasma association map. */
  HASH_ITER(local_scheduler_plasma_hh, state->local_scheduler_plasma_map, entry,
            tmp) {
    HASH_DELETE(local_scheduler_plasma_hh, state->local_scheduler_plasma_map,
                entry);
    /* Now free the shared hash entry -- no longer needed. */
    free(entry);
  }

  /* Free the scheduler object info table. */
  SchedulerObjectInfo *object_entry, *tmp_entry;
  HASH_ITER(hh, state->scheduler_object_info_table, object_entry, tmp_entry) {
    HASH_DELETE(hh, state->scheduler_object_info_table, object_entry);
    utarray_free(object_entry->object_locations);
    free(object_entry);
  }
  /* Free the array of unschedulable tasks. */
  int64_t num_pending_tasks = utarray_len(state->pending_tasks);
  if (num_pending_tasks > 0) {
    LOG_WARN("There are %" PRId64
             " remaining tasks in the pending tasks array.",
             num_pending_tasks);
  }
  for (int i = 0; i < num_pending_tasks; ++i) {
    Task **pending_task = (Task **) utarray_eltptr(state->pending_tasks, i);
    Task_free(*pending_task);
  }
  utarray_free(state->pending_tasks);
  /* Free the global scheduler state. */
  free(state);
}

/* We need this code so we can clean up when we get a SIGTERM signal. */

GlobalSchedulerState *g_state;

void signal_handler(int signal) {
  if (signal == SIGTERM) {
    GlobalSchedulerState_free(g_state);
    exit(0);
  }
}

/* End of the cleanup code. */

LocalScheduler *get_local_scheduler(GlobalSchedulerState *state,
                                    DBClientID local_scheduler_id) {
  LocalScheduler *local_scheduler_ptr;
  for (int i = 0; i < utarray_len(state->local_schedulers); ++i) {
    local_scheduler_ptr =
        (LocalScheduler *) utarray_eltptr(state->local_schedulers, i);
    if (DBClientID_equal(local_scheduler_ptr->id, local_scheduler_id)) {
      LOG_DEBUG("local_scheduler_id matched cached local scheduler entry.");
      return local_scheduler_ptr;
    }
  }
  return NULL;
}

void process_task_waiting(Task *waiting_task, void *user_context) {
  GlobalSchedulerState *state = (GlobalSchedulerState *) user_context;
  LOG_DEBUG("Task waiting callback is called.");
  bool successfully_assigned =
      handle_task_waiting(state, state->policy_state, waiting_task);
  /* If the task was not successfully submitted to a local scheduler, add the
   * task to the array of pending tasks. The global scheduler will periodically
   * resubmit the tasks in this array. */
  if (!successfully_assigned) {
    Task *task_copy = Task_copy(waiting_task);
    utarray_push_back(state->pending_tasks, &task_copy);
  }
}

void add_local_scheduler(GlobalSchedulerState *state,
                         DBClientID db_client_id,
                         const char *aux_address) {
  /* Add plasma_manager ip:port -> local_scheduler_db_client_id association to
   * state. */
  AuxAddressEntry *plasma_local_scheduler_entry =
      (AuxAddressEntry *) calloc(1, sizeof(AuxAddressEntry));
  plasma_local_scheduler_entry->aux_address = strdup(aux_address);
  plasma_local_scheduler_entry->local_scheduler_db_client_id = db_client_id;
  HASH_ADD_KEYPTR(plasma_local_scheduler_hh, state->plasma_local_scheduler_map,
                  plasma_local_scheduler_entry->aux_address,
                  strlen(plasma_local_scheduler_entry->aux_address),
                  plasma_local_scheduler_entry);

  /* Add local_scheduler_db_client_id -> plasma_manager ip:port association to
   * state. */
  HASH_ADD(local_scheduler_plasma_hh, state->local_scheduler_plasma_map,
           local_scheduler_db_client_id,
           sizeof(plasma_local_scheduler_entry->local_scheduler_db_client_id),
           plasma_local_scheduler_entry);

#if (RAY_COMMON_LOG_LEVEL <= RAY_COMMON_DEBUG)
  {
    /* Print the local scheduler to plasma association map so far. */
    AuxAddressEntry *entry, *tmp;
    LOG_DEBUG("Local scheduler to plasma hash map so far:");
    HASH_ITER(plasma_local_scheduler_hh, state->plasma_local_scheduler_map,
              entry, tmp) {
      LOG_DEBUG("%s -> %s", entry->aux_address,
                ObjectID_to_string(entry->local_scheduler_db_client_id,
                                   id_string, ID_STRING_SIZE));
    }
  }
#endif

  /* Add new local scheduler to the state. */
  LocalScheduler local_scheduler;
  local_scheduler.id = db_client_id;
  local_scheduler.num_heartbeats_missed = 0;
  local_scheduler.num_tasks_sent = 0;
  local_scheduler.num_recent_tasks_sent = 0;
  local_scheduler.info.task_queue_length = 0;
  local_scheduler.info.available_workers = 0;
  memset(local_scheduler.info.dynamic_resources, 0,
         sizeof(local_scheduler.info.dynamic_resources));
  memset(local_scheduler.info.static_resources, 0,
         sizeof(local_scheduler.info.static_resources));
  utarray_push_back(state->local_schedulers, &local_scheduler);

  /* Allow the scheduling algorithm to process this event. */
  handle_new_local_scheduler(state, state->policy_state, db_client_id);
}

void remove_local_scheduler(GlobalSchedulerState *state, int index) {
  LocalScheduler *active_worker =
      (LocalScheduler *) utarray_eltptr(state->local_schedulers, index);
  DBClientID db_client_id = active_worker->id;
  utarray_erase(state->local_schedulers, index, 1);

  AuxAddressEntry *entry, *tmp;
  HASH_ITER(plasma_local_scheduler_hh, state->plasma_local_scheduler_map, entry,
            tmp) {
    if (DBClientID_equal(entry->local_scheduler_db_client_id, db_client_id)) {
      HASH_DELETE(plasma_local_scheduler_hh, state->plasma_local_scheduler_map,
                  entry);
      /* The hash entry is shared with the local_scheduler_plasma hashmap and
       * will be freed there. */
      free(entry->aux_address);
    }
  }

  HASH_FIND(local_scheduler_plasma_hh, state->local_scheduler_plasma_map,
            &db_client_id, sizeof(db_client_id), entry);
  CHECK(entry != NULL);
  HASH_DELETE(local_scheduler_plasma_hh, state->local_scheduler_plasma_map,
              entry);
  free(entry);

  handle_local_scheduler_removed(state, state->policy_state, db_client_id);
}

/**
 * Process a notification about a new DB client connecting to Redis.
 * @param aux_address: an ip:port pair for the plasma manager associated with
 * this db client.
 */
void process_new_db_client(DBClientID db_client_id,
                           const char *client_type,
                           const char *aux_address,
                           bool is_insertion,
                           void *user_context) {
  GlobalSchedulerState *state = (GlobalSchedulerState *) user_context;
  char id_string[ID_STRING_SIZE];
  LOG_DEBUG("db client table callback for db client = %s",
            ObjectID_to_string(db_client_id, id_string, ID_STRING_SIZE));
  UNUSED(id_string);
  if (strncmp(client_type, "local_scheduler", strlen("local_scheduler")) == 0) {
    if (is_insertion) {
      /* This is a notification for an insert. */
      add_local_scheduler(state, db_client_id, aux_address);
    } else {
      int i = 0;
      for (; i < utarray_len(state->local_schedulers); ++i) {
        LocalScheduler *active_worker =
            (LocalScheduler *) utarray_eltptr(state->local_schedulers, i);
        if (DBClientID_equal(active_worker->id, db_client_id)) {
          break;
        }
      }
      if (i < utarray_len(state->local_schedulers)) {
        remove_local_scheduler(state, i);
      }
    }
  }
}

/**
 * Process notification about the new object information.
 *
 * @param object_id ID of the object that the notification is about.
 * @param data_size The object size.
 * @param manager_count The number of locations for this object.
 * @param manager_vector The vector of Plasma Manager locations.
 * @param user_context The user context.
 * @return Void.
 */
void object_table_subscribe_callback(ObjectID object_id,
                                     int64_t data_size,
                                     int manager_count,
                                     const char *manager_vector[],
                                     void *user_context) {
  /* Extract global scheduler state from the callback context. */
  GlobalSchedulerState *state = (GlobalSchedulerState *) user_context;
  char id_string[ID_STRING_SIZE];
  LOG_DEBUG("object table subscribe callback for OBJECT = %s",
            ObjectID_to_string(object_id, id_string, ID_STRING_SIZE));
  UNUSED(id_string);
  LOG_DEBUG("\tManagers<%d>:", manager_count);
  for (int i = 0; i < manager_count; i++) {
    LOG_DEBUG("\t\t%s", manager_vector[i]);
  }
  SchedulerObjectInfo *obj_info_entry = NULL;

  HASH_FIND(hh, state->scheduler_object_info_table, &object_id,
            sizeof(object_id), obj_info_entry);

  if (obj_info_entry == NULL) {
    /* Construct a new object info hash table entry. */
    obj_info_entry =
        (SchedulerObjectInfo *) malloc(sizeof(SchedulerObjectInfo));
    memset(obj_info_entry, 0, sizeof(*obj_info_entry));

    obj_info_entry->object_id = object_id;
    obj_info_entry->data_size = data_size;

    HASH_ADD(hh, state->scheduler_object_info_table, object_id,
             sizeof(obj_info_entry->object_id), obj_info_entry);
    LOG_DEBUG("New object added to object_info_table with id = %s",
              ObjectID_to_string(object_id, id_string, ID_STRING_SIZE));
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

void local_scheduler_table_handler(DBClientID client_id,
                                   LocalSchedulerInfo info,
                                   void *user_context) {
  /* Extract global scheduler state from the callback context. */
  GlobalSchedulerState *state = (GlobalSchedulerState *) user_context;
  UNUSED(state);
  char id_string[ID_STRING_SIZE];
  LOG_DEBUG(
      "Local scheduler heartbeat from db_client_id %s",
      ObjectID_to_string((ObjectID) client_id, id_string, ID_STRING_SIZE));
  UNUSED(id_string);
  LOG_DEBUG(
      "total workers = %d, task queue length = %d, available workers = %d",
      info.total_num_workers, info.task_queue_length, info.available_workers);
  /* Update the local scheduler info struct. */
  LocalScheduler *local_scheduler_ptr = get_local_scheduler(state, client_id);
  if (local_scheduler_ptr != NULL) {
    /* Reset the number of tasks sent since the last heartbeat. */
    local_scheduler_ptr->num_heartbeats_missed = 0;
    local_scheduler_ptr->num_recent_tasks_sent = 0;
    local_scheduler_ptr->info = info;
  } else {
    LOG_WARN("client_id didn't match any cached local scheduler entries");
  }
}

int task_cleanup_handler(event_loop *loop, timer_id id, void *context) {
  GlobalSchedulerState *state = (GlobalSchedulerState *) context;
  /* Loop over the pending tasks and resubmit them. */
  int64_t num_pending_tasks = utarray_len(state->pending_tasks);
  for (int64_t i = num_pending_tasks - 1; i >= 0; --i) {
    Task **pending_task = (Task **) utarray_eltptr(state->pending_tasks, i);
    /* Pretend that the task has been resubmitted. */
    bool successfully_assigned =
        handle_task_waiting(state, state->policy_state, *pending_task);
    if (successfully_assigned) {
      /* The task was successfully assigned, so remove it from this list and
       * free it. */
      utarray_erase(state->pending_tasks, i, 1);
      free(*pending_task);
    }
  }

  return GLOBAL_SCHEDULER_TASK_CLEANUP_MILLISECONDS;
}

int heartbeat_timeout_handler(event_loop *loop, timer_id id, void *context) {
  GlobalSchedulerState *state = (GlobalSchedulerState *) context;
  /* Check for local schedulers that have missed a number of heartbeats. If any
   * local schedulers have died, notify others so that the state can be cleaned
   * up. */
  /* TODO(swang): If the local scheduler hasn't actually died, then it should
   * clean up its state and exit upon receiving this notification. */
  LocalScheduler *local_scheduler_ptr;
  for (int i = utarray_len(state->local_schedulers) - 1; i >= 0; --i) {
    local_scheduler_ptr =
        (LocalScheduler *) utarray_eltptr(state->local_schedulers, i);
    if (local_scheduler_ptr->num_heartbeats_missed >= NUM_HEARTBEATS_TIMEOUT) {
      LOG_WARN(
          "Missed too many heartbeats from local scheduler, marking as dead.");
      /* Notify others by updating the global state. */
      db_client_table_remove(state->db, local_scheduler_ptr->id, NULL, NULL,
                             NULL);
      /* Remove the scheduler from the local state. */
      remove_local_scheduler(state, i);
    }
    ++local_scheduler_ptr->num_heartbeats_missed;
  }

  /* Reset the timer. */
  return HEARTBEAT_TIMEOUT_MILLISECONDS;
}

void start_server(const char *node_ip_address,
                  const char *redis_primary_addr,
                  int redis_primary_port,
                  const std::vector<std::string>& redis_shards_addrs,
                  const std::vector<int> &redis_shards_ports) {
  event_loop *loop = event_loop_create();
  g_state =
      GlobalSchedulerState_init(loop, node_ip_address,
                                redis_primary_addr, redis_primary_port,
                                redis_shards_addrs, redis_shards_ports);
  /* TODO(rkn): subscribe to notifications from the object table. */
  /* Subscribe to notifications about new local schedulers. TODO(rkn): this
   * needs to also get all of the clients that registered with the database
   * before this call to subscribe. */
  db_client_table_subscribe(g_state->db, process_new_db_client,
                            (void *) g_state, NULL, NULL, NULL);
  /* Subscribe to notifications about waiting tasks. TODO(rkn): this may need to
   * get tasks that were submitted to the database before the subscribe. */
  task_table_subscribe(g_state->db, NIL_ID, TASK_STATUS_WAITING,
                       process_task_waiting, (void *) g_state, NULL, NULL,
                       NULL);

  object_table_subscribe_to_notifications(g_state->db, true,
                                          object_table_subscribe_callback,
                                          g_state, NULL, NULL, NULL);
  /* Subscribe to notifications from local schedulers. These notifications serve
   * as heartbeats and contain informaion about the load on the local
   * schedulers. */
  local_scheduler_table_subscribe(g_state->db, local_scheduler_table_handler,
                                  g_state, NULL);
  /* Start a timer that periodically checks if there are queued tasks that can
   * be scheduled. Currently this is only used to handle the special case in
   * which a task is waiting and no node meets its static resource requirements.
   * If a new node joins the cluster that does have enough resources, then this
   * timer should notice and schedule the task. */
  event_loop_add_timer(loop, GLOBAL_SCHEDULER_TASK_CLEANUP_MILLISECONDS,
                       task_cleanup_handler, g_state);
  event_loop_add_timer(loop, HEARTBEAT_TIMEOUT_MILLISECONDS,
                       heartbeat_timeout_handler, g_state);
  /* Start the event loop. */
  event_loop_run(loop);
}

int main(int argc, char *argv[]) {
  signal(SIGTERM, signal_handler);
  /* IP address and port of the primary redis instance. */
  char *redis_primary_addr_port = NULL;
  /* IP addresses and ports of the other redis shards. */
  char *redis_shards_addrs_ports = NULL;
  /* The IP address of the node that this global scheduler is running on. */
  char *node_ip_address = NULL;
  int c;
  while ((c = getopt(argc, argv, "h:r:t:")) != -1) {
    switch (c) {
    case 'r':
      redis_primary_addr_port = optarg;
      break;
    case 't':
      redis_shards_addrs_ports = optarg;
      break;
    case 'h':
      node_ip_address = optarg;
      break;
    default:
      LOG_ERROR("unknown option %c", c);
      exit(-1);
    }
  }

  char redis_primary_addr[16];
  int redis_primary_port;
  std::vector<std::string> redis_shards_ip_addrs;
  std::vector<int> redis_shards_ports;

  if (!redis_primary_addr_port ||
      parse_ip_addr_port(redis_primary_addr_port, redis_primary_addr, &redis_primary_port) == -1) {
    LOG_FATAL(
        "specify the primary redis address like 127.0.0.1:6379 with the -r switch");
  }
  if (!redis_shards_addrs_ports ||
       !parse_ip_addrs_ports(std::string(redis_shards_addrs_ports), redis_shards_ip_addrs, redis_shards_ports)) {
    LOG_FATAL(
        "specify the redis shards like [127.0.0.1:6380,127.0.0.1:6381] with the -t switch");
  }
  if (!node_ip_address) {
    LOG_FATAL("specify the node IP address with the -h switch");
  }
  start_server(node_ip_address, redis_primary_addr, redis_primary_port, redis_shards_ip_addrs, redis_shards_ports);
}
