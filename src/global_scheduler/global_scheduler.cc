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

/**
 * Retry the task assignment. If the local scheduler that the task is assigned
 * to is no longer active, do not retry the assignment.
 * TODO(rkn): We currently only retry the method if the global scheduler
 * publishes a task to a local scheduler before the local scheduler has
 * subscribed to the channel. If we enforce that ordering, we can remove this
 * retry method.
 *
 * @param id The task ID.
 * @param user_context The global scheduler state.
 * @param user_data The Task that failed to be assigned.
 * @return Void.
 */
void assign_task_to_local_scheduler_retry(UniqueID id,
                                          void *user_context,
                                          void *user_data) {
  GlobalSchedulerState *state = (GlobalSchedulerState *) user_context;
  Task *task = (Task *) user_data;
  RAY_CHECK(Task_state(task) == TaskStatus::SCHEDULED);

  // If the local scheduler has died since we requested the task assignment, do
  // not retry again.
  DBClientID local_scheduler_id = Task_local_scheduler(task);
  auto it = state->local_schedulers.find(local_scheduler_id);
  if (it == state->local_schedulers.end()) {
    return;
  }

#if !RAY_USE_NEW_GCS
  // The local scheduler is still alive. The failure is most likely due to the
  // task assignment getting published before the local scheduler subscribed to
  // the channel. Retry the assignment.
  auto retryInfo = RetryInfo{
      .num_retries = 0,  // This value is unused.
      .timeout = 0,      // This value is unused.
      .fail_callback = assign_task_to_local_scheduler_retry,
  };
  task_table_update(state->db, Task_copy(task), &retryInfo, NULL, user_context);
#else
  RAY_CHECK_OK(TaskTableAdd(&state->gcs_client, task));
#endif
}

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
  TaskSpec *spec = Task_task_execution_spec(task)->Spec();
  RAY_LOG(DEBUG) << "assigning task to local_scheduler_id = "
                 << local_scheduler_id;
  Task_set_state(task, TaskStatus::SCHEDULED);
  Task_set_local_scheduler(task, local_scheduler_id);
  RAY_LOG(DEBUG) << "Issuing a task table update for task = "
                 << Task_task_id(task);

#if !RAY_USE_NEW_GCS
  auto retryInfo = RetryInfo{
      .num_retries = 0,  // This value is unused.
      .timeout = 0,      // This value is unused.
      .fail_callback = assign_task_to_local_scheduler_retry,
  };
  task_table_update(state->db, Task_copy(task), &retryInfo, NULL, state);
#else
  RAY_CHECK_OK(TaskTableAdd(&state->gcs_client, task));
#endif

  /* Update the object table info to reflect the fact that the results of this
   * task will be created on the machine that the task was assigned to. This can
   * be used to improve locality-aware scheduling. */
  for (int64_t i = 0; i < TaskSpec_num_returns(spec); ++i) {
    ObjectID return_id = TaskSpec_return(spec, i);
    if (state->scheduler_object_info_table.find(return_id) ==
        state->scheduler_object_info_table.end()) {
      SchedulerObjectInfo &obj_info_entry =
          state->scheduler_object_info_table[return_id];
      /* The value -1 indicates that the size of the object is not known yet. */
      obj_info_entry.data_size = -1;
    }
    RAY_CHECK(state->local_scheduler_plasma_map.count(local_scheduler_id) == 1);
    state->scheduler_object_info_table[return_id].object_locations.push_back(
        state->local_scheduler_plasma_map[local_scheduler_id]);
  }

  /* TODO(rkn): We should probably pass around local_scheduler struct pointers
   * instead of db_client_id objects. */
  /* Update the local scheduler info. */
  auto it = state->local_schedulers.find(local_scheduler_id);
  RAY_CHECK(it != state->local_schedulers.end());

  LocalScheduler &local_scheduler = it->second;
  local_scheduler.num_tasks_sent += 1;
  local_scheduler.num_recent_tasks_sent += 1;
  // Resource accounting update for this local scheduler.
  for (auto const &resource_pair : TaskSpec_get_required_resources(spec)) {
    std::string resource_name = resource_pair.first;
    double resource_quantity = resource_pair.second;
    // The local scheduler must have this resource because otherwise we wouldn't
    // be assigning the task to this local scheduler.
    RAY_CHECK(local_scheduler.info.dynamic_resources.count(resource_name) ==
                  1 ||
              resource_quantity == 0);
    // Subtract task's resource from the cached dynamic resource capacity for
    // this local scheduler. This will be overwritten on the next heartbeat.
    local_scheduler.info.dynamic_resources[resource_name] =
        MAX(0, local_scheduler.info.dynamic_resources[resource_name] -
                   resource_quantity);
  }
}

GlobalSchedulerState *GlobalSchedulerState_init(event_loop *loop,
                                                const char *node_ip_address,
                                                const char *redis_primary_addr,
                                                int redis_primary_port) {
  GlobalSchedulerState *state = new GlobalSchedulerState();
  state->loop = loop;
  state->db = db_connect(std::string(redis_primary_addr), redis_primary_port,
                         "global_scheduler", node_ip_address,
                         std::vector<std::string>());
  db_attach(state->db, loop, false);

  RAY_CHECK_OK(state->gcs_client.Connect(std::string(redis_primary_addr),
                                         redis_primary_port));
  RAY_CHECK_OK(state->gcs_client.context()->AttachToEventLoop(loop));
  state->policy_state = GlobalSchedulerPolicyState_init();
  return state;
}

void GlobalSchedulerState_free(GlobalSchedulerState *state) {
  db_disconnect(state->db);
  state->local_schedulers.clear();
  GlobalSchedulerPolicyState_free(state->policy_state);
  /* Delete the plasma to local scheduler association map. */
  state->plasma_local_scheduler_map.clear();

  /* Delete the local scheduler to plasma association map. */
  state->local_scheduler_plasma_map.clear();

  /* Free the scheduler object info table. */
  state->scheduler_object_info_table.clear();
  /* Free the array of unschedulable tasks. */
  int64_t num_pending_tasks = state->pending_tasks.size();
  if (num_pending_tasks > 0) {
    RAY_LOG(WARNING) << "There are " << num_pending_tasks
                     << " remaining tasks in the pending tasks array.";
  }
  for (int i = 0; i < num_pending_tasks; ++i) {
    Task *pending_task = state->pending_tasks[i];
    Task_free(pending_task);
  }
  state->pending_tasks.clear();

  /* Destroy the event loop. */
  destroy_outstanding_callbacks(state->loop);
  event_loop_destroy(state->loop);
  state->loop = NULL;

  /* Free the global scheduler state. */
  delete state;
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

void process_task_waiting(Task *waiting_task, void *user_context) {
  GlobalSchedulerState *state = (GlobalSchedulerState *) user_context;
  RAY_LOG(DEBUG) << "Task waiting callback is called.";
  bool successfully_assigned =
      handle_task_waiting(state, state->policy_state, waiting_task);
  /* If the task was not successfully submitted to a local scheduler, add the
   * task to the array of pending tasks. The global scheduler will periodically
   * resubmit the tasks in this array. */
  if (!successfully_assigned) {
    Task *task_copy = Task_copy(waiting_task);
    state->pending_tasks.push_back(task_copy);
  }
}

void add_local_scheduler(GlobalSchedulerState *state,
                         DBClientID db_client_id,
                         const char *manager_address) {
  /* Add plasma_manager ip:port -> local_scheduler_db_client_id association to
   * state. */
  state->plasma_local_scheduler_map[std::string(manager_address)] =
      db_client_id;

  /* Add local_scheduler_db_client_id -> plasma_manager ip:port association to
   * state. */
  state->local_scheduler_plasma_map[db_client_id] =
      std::string(manager_address);

  /* Add new local scheduler to the state. */
  LocalScheduler &local_scheduler = state->local_schedulers[db_client_id];
  local_scheduler.id = db_client_id;
  local_scheduler.num_heartbeats_missed = 0;
  local_scheduler.num_tasks_sent = 0;
  local_scheduler.num_recent_tasks_sent = 0;
  local_scheduler.info.task_queue_length = 0;
  local_scheduler.info.available_workers = 0;

  /* Allow the scheduling algorithm to process this event. */
  handle_new_local_scheduler(state, state->policy_state, db_client_id);
}

std::unordered_map<DBClientID, LocalScheduler>::iterator remove_local_scheduler(
    GlobalSchedulerState *state,
    std::unordered_map<DBClientID, LocalScheduler>::iterator it) {
  RAY_CHECK(it != state->local_schedulers.end());
  DBClientID local_scheduler_id = it->first;
  it = state->local_schedulers.erase(it);

  /* Remove the local scheduler from the mappings. This code only makes sense if
   * there is a one-to-one mapping between local schedulers and plasma managers.
   */
  std::string manager_address =
      state->local_scheduler_plasma_map[local_scheduler_id];
  state->local_scheduler_plasma_map.erase(local_scheduler_id);
  state->plasma_local_scheduler_map.erase(manager_address);

  handle_local_scheduler_removed(state, state->policy_state,
                                 local_scheduler_id);
  return it;
}

/**
 * Process a notification about a new DB client connecting to Redis.
 *
 * @param manager_address An ip:port pair for the plasma manager associated with
 *        this db client.
 * @return Void.
 */
void process_new_db_client(DBClient *db_client, void *user_context) {
  GlobalSchedulerState *state = (GlobalSchedulerState *) user_context;
  RAY_LOG(DEBUG) << "db client table callback for db client = "
                 << db_client->id;
  if (strncmp(db_client->client_type.c_str(), "local_scheduler",
              strlen("local_scheduler")) == 0) {
    bool local_scheduler_present =
        (state->local_schedulers.find(db_client->id) !=
         state->local_schedulers.end());
    if (db_client->is_alive) {
      /* This is a notification for an insert. We may receive duplicate
       * notifications since we read the entire table before processing
       * notifications. Filter out local schedulers that we already added. */
      if (!local_scheduler_present) {
        add_local_scheduler(state, db_client->id,
                            db_client->manager_address.c_str());
      }
    } else {
      if (local_scheduler_present) {
        remove_local_scheduler(state,
                               state->local_schedulers.find(db_client->id));
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
 * @param manager_ids The vector of Plasma Manager client IDs.
 * @param user_context The user context.
 * @return Void.
 */
void object_table_subscribe_callback(ObjectID object_id,
                                     int64_t data_size,
                                     const std::vector<DBClientID> &manager_ids,
                                     void *user_context) {
  /* Extract global scheduler state from the callback context. */
  GlobalSchedulerState *state = (GlobalSchedulerState *) user_context;
  RAY_LOG(DEBUG) << "object table subscribe callback for OBJECT = "
                 << object_id;

  const std::vector<std::string> managers =
      db_client_table_get_ip_addresses(state->db, manager_ids);
  RAY_LOG(DEBUG) << "\tManagers<" << managers.size() << ">:";
  for (size_t i = 0; i < managers.size(); i++) {
    RAY_LOG(DEBUG) << "\t\t" << managers[i];
  }

  if (state->scheduler_object_info_table.find(object_id) ==
      state->scheduler_object_info_table.end()) {
    /* Construct a new object info hash table entry. */
    SchedulerObjectInfo &obj_info_entry =
        state->scheduler_object_info_table[object_id];
    obj_info_entry.data_size = data_size;

    RAY_LOG(DEBUG) << "New object added to object_info_table with id = "
                   << object_id;
    RAY_LOG(DEBUG) << "\tmanager locations:";
    for (size_t i = 0; i < managers.size(); i++) {
      RAY_LOG(DEBUG) << "\t\t" << managers[i];
    }
  }

  SchedulerObjectInfo &obj_info_entry =
      state->scheduler_object_info_table[object_id];

  /* In all cases, replace the object location vector on each callback. */
  obj_info_entry.object_locations.clear();
  for (size_t i = 0; i < managers.size(); i++) {
    obj_info_entry.object_locations.push_back(managers[i]);
  }
}

void local_scheduler_table_handler(DBClientID client_id,
                                   LocalSchedulerInfo info,
                                   void *user_context) {
  /* Extract global scheduler state from the callback context. */
  GlobalSchedulerState *state = (GlobalSchedulerState *) user_context;
  ARROW_UNUSED(state);
  RAY_LOG(DEBUG) << "Local scheduler heartbeat from db_client_id " << client_id;
  RAY_LOG(DEBUG) << "total workers = " << info.total_num_workers
                 << ", task queue length = " << info.task_queue_length
                 << ", available workers = " << info.available_workers;

  /* Update the local scheduler info struct. */
  auto it = state->local_schedulers.find(client_id);
  if (it != state->local_schedulers.end()) {
    if (info.is_dead) {
      /* The local scheduler is exiting. Increase the number of heartbeats
       * missed to the timeout threshold. This will trigger removal of the
       * local scheduler the next time the timeout handler fires. */
      it->second.num_heartbeats_missed =
          RayConfig::instance().num_heartbeats_timeout();
    } else {
      /* Reset the number of tasks sent since the last heartbeat. */
      LocalScheduler &local_scheduler = it->second;
      local_scheduler.num_heartbeats_missed = 0;
      local_scheduler.num_recent_tasks_sent = 0;
      local_scheduler.info = info;
    }
  } else {
    RAY_LOG(WARNING) << "client_id didn't match any cached local scheduler "
                     << "entries";
  }
}

int task_cleanup_handler(event_loop *loop, timer_id id, void *context) {
  GlobalSchedulerState *state = (GlobalSchedulerState *) context;
  /* Loop over the pending tasks in reverse order and resubmit them. */
  auto it = state->pending_tasks.end();
  while (it != state->pending_tasks.begin()) {
    it--;
    Task *pending_task = *it;
    /* Pretend that the task has been resubmitted. */
    bool successfully_assigned =
        handle_task_waiting(state, state->policy_state, pending_task);
    if (successfully_assigned) {
      /* The task was successfully assigned, so remove it from this list and
       * free it. This uses the fact that pending_tasks is a vector and so erase
       * returns an iterator to the next element in the vector. */
      it = state->pending_tasks.erase(it);
      Task_free(pending_task);
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
  auto it = state->local_schedulers.begin();
  while (it != state->local_schedulers.end()) {
    if (it->second.num_heartbeats_missed >=
        RayConfig::instance().num_heartbeats_timeout()) {
      RAY_LOG(WARNING) << "Missed too many heartbeats from local scheduler, "
                       << "marking as dead.";
      /* Notify others by updating the global state. */
      db_client_table_remove(state->db, it->second.id, NULL, NULL, NULL);
      /* Remove the scheduler from the local state. The call to
       * remove_local_scheduler modifies the container in place and returns the
       * next iterator. */
      it = remove_local_scheduler(state, it);
    } else {
      it->second.num_heartbeats_missed += 1;
      it++;
    }
  }

  /* Reset the timer. */
  return RayConfig::instance().heartbeat_timeout_milliseconds();
}

void start_server(const char *node_ip_address,
                  const char *redis_primary_addr,
                  int redis_primary_port) {
  event_loop *loop = event_loop_create();
  g_state = GlobalSchedulerState_init(loop, node_ip_address, redis_primary_addr,
                                      redis_primary_port);
  /* TODO(rkn): subscribe to notifications from the object table. */
  /* Subscribe to notifications about new local schedulers. TODO(rkn): this
   * needs to also get all of the clients that registered with the database
   * before this call to subscribe. */
  db_client_table_subscribe(g_state->db, process_new_db_client,
                            (void *) g_state, NULL, NULL, NULL);
  /* Subscribe to notifications about waiting tasks. If a local scheduler
   * submits tasks to the global scheduler before the global scheduler
   * successfully subscribes, then the local scheduler that submitted the tasks
   * will retry. */
  task_table_subscribe(g_state->db, UniqueID::nil(), TaskStatus::WAITING,
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
  event_loop_add_timer(loop,
                       RayConfig::instance().heartbeat_timeout_milliseconds(),
                       heartbeat_timeout_handler, g_state);
  /* Start the event loop. */
  event_loop_run(loop);
}

int main(int argc, char *argv[]) {
  signal(SIGTERM, signal_handler);
  /* IP address and port of the primary redis instance. */
  char *redis_primary_addr_port = NULL;
  /* The IP address of the node that this global scheduler is running on. */
  char *node_ip_address = NULL;
  int c;
  while ((c = getopt(argc, argv, "h:r:")) != -1) {
    switch (c) {
    case 'r':
      redis_primary_addr_port = optarg;
      break;
    case 'h':
      node_ip_address = optarg;
      break;
    default:
      RAY_LOG(FATAL) << "unknown option " << c;
    }
  }

  char redis_primary_addr[16];
  int redis_primary_port = -1;
  if (!redis_primary_addr_port ||
      parse_ip_addr_port(redis_primary_addr_port, redis_primary_addr,
                         &redis_primary_port) == -1) {
    RAY_LOG(FATAL) << "specify the primary redis address like 127.0.0.1:6379 "
                   << "with the -r switch";
  }
  if (!node_ip_address) {
    RAY_LOG(FATAL) << "specify the node IP address with the -h switch";
  }
  start_server(node_ip_address, redis_primary_addr, redis_primary_port);
}
