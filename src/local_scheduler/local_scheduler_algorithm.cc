#include "local_scheduler_algorithm.h"

#include <stdbool.h>
#include <list>
#include <vector>
#include "utarray.h"
#include "utlist.h"

#include "state/task_table.h"
#include "state/local_scheduler_table.h"
#include "state/object_table.h"
#include "local_scheduler_shared.h"
#include "local_scheduler.h"
#include "common/task.h"

/* Declared for convenience. */
void remove_actor(SchedulingAlgorithmState *algorithm_state, ActorID actor_id);

struct TaskQueueEntry {
  /** The task that is queued. */
  TaskSpec *spec;
  int64_t task_spec_size;
};

/** A data structure used to track which objects are available locally and
 *  which objects are being actively fetched. */
typedef struct {
  /** Object id of this object. */
  ObjectID object_id;
  /** A vector of tasks dependent on this object. These tasks are a subset of
   *  the tasks in the waiting queue. Each element actually stores a reference
   *  to the corresponding task's queue entry in waiting queue, for fast
   *  deletion when all of the task's dependencies become available. */
  std::vector<std::list<TaskQueueEntry>::iterator> *dependent_tasks;
  /** Handle for the uthash table. NOTE: This handle is used for both the
   *  scheduling algorithm state's local_objects and remote_objects tables.
   *  We must enforce the uthash invariant that the entry be in at most one of
   *  the tables. */
  UT_hash_handle hh;
} object_entry;

/** This is used to define the queue of actor task specs for which the
 *  corresponding local scheduler is unknown. */
UT_icd task_spec_icd = {sizeof(TaskSpec *), NULL, NULL, NULL};
/** This is used to keep track of task spec sizes in the above queue. */
UT_icd task_spec_size_icd = {sizeof(int64_t), NULL, NULL, NULL};
/** This is used to define the queue of available workers. */
UT_icd worker_icd = {sizeof(LocalSchedulerClient *), NULL, NULL, NULL};

/** This struct contains information about a specific actor. This struct will be
 *  used inside of a hash table. */
typedef struct {
  /** The ID of the actor. This is used as a key in the hash table. */
  ActorID actor_id;
  /** The number of tasks that have been executed on this actor so far. This is
   *  used to guarantee the in-order execution of tasks on actors (in the order
   *  that the tasks were submitted). This is currently meaningful because we
   *  restrict the submission of tasks on actors to the process that created the
   *  actor. */
  int64_t task_counter;
  /** A queue of tasks to be executed on this actor. The tasks will be sorted by
   *  the order of their actor counters. */
  std::list<TaskQueueEntry> *task_queue;
  /** The worker that the actor is running on. */
  LocalSchedulerClient *worker;
  /** True if the worker is available and false otherwise. */
  bool worker_available;
  /** Handle for the uthash table. */
  UT_hash_handle hh;
} LocalActorInfo;

/** Part of the local scheduler state that is maintained by the scheduling
 *  algorithm. */
struct SchedulingAlgorithmState {
  /** An array of pointers to tasks that are waiting for dependencies. */
  std::list<TaskQueueEntry> *waiting_task_queue;
  /** An array of pointers to tasks whose dependencies are ready but that are
   *  waiting to be assigned to a worker. */
  std::list<TaskQueueEntry> *dispatch_task_queue;
  /** This is a hash table from actor ID to information about that actor. In
   *  particular, a queue of tasks that are waiting to execute on that actor.
   *  This is only used for actors that exist locally. */
  LocalActorInfo *local_actor_infos;
  /** An array of actor tasks that have been submitted but this local scheduler
   *  doesn't know which local scheduler is responsible for them, so cannot
   *  assign them to the correct local scheduler yet. Whenever a notification
   *  about a new local scheduler arrives, we will resubmit all of these tasks
   *  locally. */
  UT_array *cached_submitted_actor_tasks;
  /** An array of task sizes of cached_submitted_actor_tasks. */
  UT_array *cached_submitted_actor_task_sizes;
  /** An array of pointers to workers in the worker pool. These are workers
   *  that have registered a PID with us and that are now waiting to be
   *  assigned a task to execute. */
  UT_array *available_workers;
  /** An array of pointers to workers that are currently executing a task,
   *  unblocked. These are the workers that are leasing some number of
   *  resources. */
  UT_array *executing_workers;
  /** An array of pointers to workers that are currently executing a task,
   *  blocked on some object(s) that isn't available locally yet. These are the
   *  workers that are executing a task, but that have temporarily returned the
   *  task's required resources. */
  UT_array *blocked_workers;
  /** A hash map of the objects that are available in the local Plasma store.
   *  The key is the object ID. This information could be a little stale. */
  object_entry *local_objects;
  /** A hash map of the objects that are not available locally. These are
   *  currently being fetched by this local scheduler. The key is the object
   *  ID. Every LOCAL_SCHEDULER_FETCH_TIMEOUT_MILLISECONDS, a Plasma fetch
   *  request will be sent the object IDs in this table. Each entry also holds
   *  an array of queued tasks that are dependent on it. */
  object_entry *remote_objects;
};

TaskQueueEntry TaskQueueEntry_init(TaskSpec *spec, int64_t task_spec_size) {
  TaskQueueEntry elt;
  elt.spec = (TaskSpec *) malloc(task_spec_size);
  memcpy(elt.spec, spec, task_spec_size);
  elt.task_spec_size = task_spec_size;
  return elt;
}

void TaskQueueEntry_free(TaskQueueEntry *entry) {
  TaskSpec_free(entry->spec);
}

SchedulingAlgorithmState *SchedulingAlgorithmState_init(void) {
  SchedulingAlgorithmState *algorithm_state =
      (SchedulingAlgorithmState *) malloc(sizeof(SchedulingAlgorithmState));
  /* Initialize an empty hash map for the cache of local available objects. */
  algorithm_state->local_objects = NULL;
  /* Initialize the hash table of objects being fetched. */
  algorithm_state->remote_objects = NULL;
  /* Initialize the local data structures used for queuing tasks and workers. */
  algorithm_state->waiting_task_queue = new std::list<TaskQueueEntry>();
  algorithm_state->dispatch_task_queue = new std::list<TaskQueueEntry>();

  utarray_new(algorithm_state->cached_submitted_actor_tasks, &task_spec_icd);
  utarray_new(algorithm_state->cached_submitted_actor_task_sizes,
              &task_spec_size_icd);

  algorithm_state->local_actor_infos = NULL;

  utarray_new(algorithm_state->available_workers, &worker_icd);
  utarray_new(algorithm_state->executing_workers, &worker_icd);
  utarray_new(algorithm_state->blocked_workers, &worker_icd);
  return algorithm_state;
}

void SchedulingAlgorithmState_free(SchedulingAlgorithmState *algorithm_state) {
  /* Free all of the tasks in the waiting queue. */
  for (auto &task : *algorithm_state->waiting_task_queue) {
    TaskQueueEntry_free(&task);
  }
  algorithm_state->waiting_task_queue->clear();
  delete algorithm_state->waiting_task_queue;
  /* Free all the tasks in the dispatch queue. */
  for (auto &task : *algorithm_state->dispatch_task_queue) {
    TaskQueueEntry_free(&task);
  }
  algorithm_state->dispatch_task_queue->clear();
  delete algorithm_state->dispatch_task_queue;
  /* Remove all of the remaining actors. */
  LocalActorInfo *actor_entry, *tmp_actor_entry;
  HASH_ITER(hh, algorithm_state->local_actor_infos, actor_entry,
            tmp_actor_entry) {
    /* We do not call HASH_DELETE here because it will be called inside of
     * remove_actor. */
    remove_actor(algorithm_state, actor_entry->actor_id);
  }
  /* Free the list of cached actor task specs and the task specs themselves. */
  for (int i = 0;
       i < utarray_len(algorithm_state->cached_submitted_actor_tasks); ++i) {
    TaskSpec **spec = (TaskSpec **) utarray_eltptr(
        algorithm_state->cached_submitted_actor_tasks, i);
    free(*spec);
  }
  utarray_free(algorithm_state->cached_submitted_actor_tasks);
  utarray_free(algorithm_state->cached_submitted_actor_task_sizes);
  /* Free the list of available workers. */
  utarray_free(algorithm_state->available_workers);
  utarray_free(algorithm_state->executing_workers);
  utarray_free(algorithm_state->blocked_workers);
  /* Free the cached information about which objects are present locally. */
  object_entry *obj_entry, *tmp_obj_entry;
  HASH_ITER(hh, algorithm_state->local_objects, obj_entry, tmp_obj_entry) {
    HASH_DELETE(hh, algorithm_state->local_objects, obj_entry);
    CHECK(obj_entry->dependent_tasks->empty());
    delete obj_entry->dependent_tasks;
    free(obj_entry);
  }
  /* Free the cached information about which objects are currently being
   * fetched. */
  HASH_ITER(hh, algorithm_state->remote_objects, obj_entry, tmp_obj_entry) {
    HASH_DELETE(hh, algorithm_state->remote_objects, obj_entry);
    delete obj_entry->dependent_tasks;
    free(obj_entry);
  }
  /* Free the algorithm state. */
  free(algorithm_state);
}

void provide_scheduler_info(LocalSchedulerState *state,
                            SchedulingAlgorithmState *algorithm_state,
                            LocalSchedulerInfo *info) {
  info->total_num_workers = utarray_len(state->workers);
  /* TODO(swang): Provide separate counts for tasks that are waiting for
   * dependencies vs tasks that are waiting to be assigned. */
  int64_t waiting_task_queue_length =
      algorithm_state->waiting_task_queue->size();
  int64_t dispatch_task_queue_length =
      algorithm_state->dispatch_task_queue->size();
  info->task_queue_length =
      waiting_task_queue_length + dispatch_task_queue_length;
  info->available_workers = utarray_len(algorithm_state->available_workers);
  /* Copy static and dynamic resource information. */
  for (int i = 0; i < ResourceIndex_MAX; i++) {
    info->dynamic_resources[i] = state->dynamic_resources[i];
    info->static_resources[i] = state->static_resources[i];
  }
}

/**
 * Create the LocalActorInfo struct for an actor worker that this local
 * scheduler is responsible for. For a given actor, this will either be done
 * when the first task for that actor arrives or when the worker running that
 * actor connects to the local scheduler.
 *
 * @param algorithm_state The state of the scheduling algorithm.
 * @param actor_id The actor ID of the actor being created.
 * @param worker The worker struct for the worker that is running this actor.
 *        If the worker struct has not been created yet (meaning that the worker
 *        that is running this actor has not registered with the local scheduler
 *        yet, and so create_actor is being called because a task for that actor
 *        has arrived), then this should be NULL.
 * @return Void.
 */
void create_actor(SchedulingAlgorithmState *algorithm_state,
                  ActorID actor_id,
                  LocalSchedulerClient *worker) {
  /* This will be freed when the actor is removed in remove_actor. */
  LocalActorInfo *entry = (LocalActorInfo *) malloc(sizeof(LocalActorInfo));
  entry->actor_id = actor_id;
  entry->task_counter = 0;
  /* Initialize the doubly-linked list to NULL. */
  entry->task_queue = new std::list<TaskQueueEntry>();
  entry->worker = worker;
  entry->worker_available = false;
  HASH_ADD(hh, algorithm_state->local_actor_infos, actor_id, sizeof(actor_id),
           entry);

  /* Log some useful information about the actor that we created. */
  char id_string[ID_STRING_SIZE];
  LOG_DEBUG("Creating actor with ID %s.",
            ObjectID_to_string(actor_id, id_string, ID_STRING_SIZE));
  UNUSED(id_string);
}

void remove_actor(SchedulingAlgorithmState *algorithm_state, ActorID actor_id) {
  LocalActorInfo *entry;
  HASH_FIND(hh, algorithm_state->local_actor_infos, &actor_id, sizeof(actor_id),
            entry);
  /* Make sure the actor actually exists. */
  CHECK(entry != NULL);

  /* Log some useful information about the actor that we're removing. */
  char id_string[ID_STRING_SIZE];
  size_t count = entry->task_queue->size();
  if (count > 0) {
    LOG_WARN("Removing actor with ID %s and %lld remaining tasks.",
             ObjectID_to_string(actor_id, id_string, ID_STRING_SIZE),
             (long long) count);
  }
  UNUSED(id_string);

  /* Free all remaining tasks in the actor queue. */
  for (auto &task : *entry->task_queue) {
    TaskQueueEntry_free(&task);
  }
  entry->task_queue->clear();
  delete entry->task_queue;
  /* Remove the entry from the hash table and free it. */
  HASH_DELETE(hh, algorithm_state->local_actor_infos, entry);
  free(entry);
}

void handle_actor_worker_connect(LocalSchedulerState *state,
                                 SchedulingAlgorithmState *algorithm_state,
                                 ActorID actor_id,
                                 LocalSchedulerClient *worker) {
  LocalActorInfo *entry;
  HASH_FIND(hh, algorithm_state->local_actor_infos, &actor_id, sizeof(actor_id),
            entry);
  if (entry == NULL) {
    create_actor(algorithm_state, actor_id, worker);
  } else {
    /* In this case, the LocalActorInfo struct was already been created by the
     * first call to add_task_to_actor_queue. However, the worker field was not
     * filled out, so fill out the correct worker field now. */
    entry->worker = worker;
  }
}

void handle_actor_worker_disconnect(LocalSchedulerState *state,
                                    SchedulingAlgorithmState *algorithm_state,
                                    ActorID actor_id) {
  remove_actor(algorithm_state, actor_id);
}

/**
 * This will add a task to the task queue for an actor. If this is the first
 * task being processed for this actor, it is possible that the LocalActorInfo
 * struct has not yet been created by create_worker (which happens when the
 * actor worker connects to the local scheduler), so in that case this method
 * will call create_actor.
 *
 * This method will also update the task table. TODO(rkn): Should we also update
 * the task table in the case where the tasks are cached locally?
 *
 * @param state The state of the local scheduler.
 * @param algorithm_state The state of the scheduling algorithm.
 * @param spec The task spec to add.
 * @param from_global_scheduler True if the task was assigned to this local
 *        scheduler by the global scheduler and false if it was submitted
 *        locally by a worker.
 * @return Void.
 */
void add_task_to_actor_queue(LocalSchedulerState *state,
                             SchedulingAlgorithmState *algorithm_state,
                             TaskSpec *spec,
                             int64_t task_spec_size,
                             bool from_global_scheduler) {
  ActorID actor_id = TaskSpec_actor_id(spec);
  char tmp[ID_STRING_SIZE];
  ObjectID_to_string(actor_id, tmp, ID_STRING_SIZE);
  DCHECK(!ActorID_equal(actor_id, NIL_ACTOR_ID));
  /* Get the local actor entry for this actor. */
  LocalActorInfo *entry;
  HASH_FIND(hh, algorithm_state->local_actor_infos, &actor_id, sizeof(actor_id),
            entry);

  /* Handle the case in which there is no LocalActorInfo struct yet. */
  if (entry == NULL) {
    /* Create the actor struct with a NULL worker because the worker struct has
     * not been created yet. The correct worker struct will be inserted when the
     * actor worker connects to the local scheduler. */
    create_actor(algorithm_state, actor_id, NULL);
    HASH_FIND(hh, algorithm_state->local_actor_infos, &actor_id,
              sizeof(actor_id), entry);
    CHECK(entry != NULL);
  }

  int64_t task_counter = TaskSpec_actor_counter(spec);
  /* As a sanity check, the counter of the new task should be greater than the
   * number of tasks that have executed on this actor so far (since we are
   * guaranteeing in-order execution of the tasks on the actor). TODO(rkn): This
   * check will fail if the fault-tolerance mechanism resubmits a task on an
   * actor. */
  CHECK(task_counter >= entry->task_counter);

  /* Create a new task queue entry. */
  TaskQueueEntry elt = TaskQueueEntry_init(spec, task_spec_size);
  /* Add the task spec to the actor's task queue in a manner that preserves the
   * order of the actor task counters. Iterate from the beginning of the queue
   * to find the right place to insert the task queue entry. TODO(pcm): This
   * makes submitting multiple actor tasks take quadratic time, which needs to
   * be optimized. */
  auto it = entry->task_queue->begin();
  while (it != entry->task_queue->end() &&
         (task_counter > TaskSpec_actor_counter(it->spec))) {
    ++it;
  }
  entry->task_queue->insert(it, elt);

  /* Update the task table. */
  if (state->db != NULL) {
    Task *task = Task_alloc(spec, task_spec_size, TASK_STATUS_QUEUED,
                            get_db_client_id(state->db));
    if (from_global_scheduler) {
      /* If the task is from the global scheduler, it's already been added to
       * the task table, so just update the entry. */
      task_table_update(state->db, task, NULL, NULL, NULL);
    } else {
      /* Otherwise, this is the first time the task has been seen in the system
       * (unless it's a resubmission of a previous task), so add the entry. */
      task_table_add_task(state->db, task, NULL, NULL, NULL);
    }
  }
}

/**
 * Dispatch a task to an actor if possible.
 *
 * @param state The state of the local scheduler.
 * @param algorithm_state The state of the scheduling algorithm.
 * @param actor_id The ID of the actor corresponding to the worker.
 * @return True if a task was dispatched to the actor and false otherwise.
 */
bool dispatch_actor_task(LocalSchedulerState *state,
                         SchedulingAlgorithmState *algorithm_state,
                         ActorID actor_id) {
  /* Make sure this worker actually is an actor. */
  CHECK(!ActorID_equal(actor_id, NIL_ACTOR_ID));
  /* Make sure this actor belongs to this local scheduler. */
  actor_map_entry *actor_entry;
  HASH_FIND(hh, state->actor_mapping, &actor_id, sizeof(actor_id), actor_entry);
  CHECK(actor_entry != NULL);
  CHECK(DBClientID_equal(actor_entry->local_scheduler_id,
                         get_db_client_id(state->db)));

  /* Get the local actor entry for this actor. */
  LocalActorInfo *entry;
  HASH_FIND(hh, algorithm_state->local_actor_infos, &actor_id, sizeof(actor_id),
            entry);
  CHECK(entry != NULL);

  if (entry->task_queue->empty()) {
    /* There are no queued tasks for this actor, so we cannot dispatch a task to
     * the actor. */
    return false;
  }
  TaskQueueEntry first_task = entry->task_queue->front();
  int64_t next_task_counter = TaskSpec_actor_counter(first_task.spec);
  if (next_task_counter != entry->task_counter) {
    /* We cannot execute the next task on this actor without violating the
     * in-order execution guarantee for actor tasks. */
    CHECK(next_task_counter > entry->task_counter);
    return false;
  }
  /* If the worker is not available, we cannot assign a task to it. */
  if (!entry->worker_available) {
    return false;
  }
  /* Assign the first task in the task queue to the worker and mark the worker
   * as unavailable. */
  entry->task_counter += 1;
  assign_task_to_worker(state, first_task.spec, first_task.task_spec_size,
                        entry->worker);
  entry->worker_available = false;
  /* Free the task queue entry. */
  TaskQueueEntry_free(&first_task);
  /* Remove the task from the actor's task queue. */
  entry->task_queue->pop_front();
  return true;
}

/**
 * Fetch a queued task's missing object dependency. The fetch request will be
 * retried every LOCAL_SCHEDULER_FETCH_TIMEOUT_MILLISECONDS until the object is
 * available locally.
 *
 * @param state The scheduler state.
 * @param algorithm_state The scheduling algorithm state.
 * @param task_entry_it A reference to the task entry in the waiting queue.
 * @param obj_id The ID of the object that the task is dependent on.
 * @returns Void.
 */
void fetch_missing_dependency(LocalSchedulerState *state,
                              SchedulingAlgorithmState *algorithm_state,
                              std::list<TaskQueueEntry>::iterator task_entry_it,
                              ObjectID obj_id) {
  object_entry *entry;
  HASH_FIND(hh, algorithm_state->remote_objects, &obj_id, sizeof(obj_id),
            entry);
  if (entry == NULL) {
    /* We weren't actively fetching this object. Try the fetch once
     * immediately. */
    if (plasma_manager_is_connected(state->plasma_conn)) {
      plasma_fetch(state->plasma_conn, 1, &obj_id);
    }
    /* Create an entry and add it to the list of active fetch requests to
     * ensure that the fetch actually happens. The entry will be moved to the
     * hash table of locally available objects in handle_object_available when
     * the object becomes available locally. It will get freed if the object is
     * subsequently removed locally. */
    entry = (object_entry *) malloc(sizeof(object_entry));
    entry->object_id = obj_id;
    entry->dependent_tasks =
        new std::vector<std::list<TaskQueueEntry>::iterator>();
    HASH_ADD(hh, algorithm_state->remote_objects, object_id,
             sizeof(entry->object_id), entry);
  }
  entry->dependent_tasks->push_back(task_entry_it);
}

/**
 * Fetch a queued task's missing object dependencies. The fetch requests will
 * be retried every LOCAL_SCHEDULER_FETCH_TIMEOUT_MILLISECONDS until all
 * objects are available locally.
 *
 * @param state The scheduler state.
 * @param algorithm_state The scheduling algorithm state.
 * @param task_entry_it A reference to the task entry in the waiting queue.
 * @returns Void.
 */
void fetch_missing_dependencies(
    LocalSchedulerState *state,
    SchedulingAlgorithmState *algorithm_state,
    std::list<TaskQueueEntry>::iterator task_entry_it) {
  TaskSpec *task = task_entry_it->spec;
  int64_t num_args = TaskSpec_num_args(task);
  int num_missing_dependencies = 0;
  for (int i = 0; i < num_args; ++i) {
    if (TaskSpec_arg_by_ref(task, i)) {
      ObjectID obj_id = TaskSpec_arg_id(task, i);
      object_entry *entry;
      HASH_FIND(hh, algorithm_state->local_objects, &obj_id, sizeof(obj_id),
                entry);
      if (entry == NULL) {
        /* If the entry is not yet available locally, record the dependency. */
        fetch_missing_dependency(state, algorithm_state, task_entry_it, obj_id);
        ++num_missing_dependencies;
      }
    }
  }
  CHECK(num_missing_dependencies > 0);
}

/**
 * Check if all of the remote object arguments for a task are available in the
 * local object store.
 *
 * @param algorithm_state The scheduling algorithm state.
 * @param task Task specification of the task to check.
 * @return bool This returns true if all of the remote object arguments for the
 *         task are present in the local object store, otherwise it returns
 *         false.
 */
bool can_run(SchedulingAlgorithmState *algorithm_state, TaskSpec *task) {
  int64_t num_args = TaskSpec_num_args(task);
  for (int i = 0; i < num_args; ++i) {
    if (TaskSpec_arg_by_ref(task, i)) {
      ObjectID obj_id = TaskSpec_arg_id(task, i);
      object_entry *entry;
      HASH_FIND(hh, algorithm_state->local_objects, &obj_id, sizeof(obj_id),
                entry);
      if (entry == NULL) {
        /* The object is not present locally, so this task cannot be scheduled
         * right now. */
        return false;
      }
    }
  }
  return true;
}

/* TODO(swang): This method is not covered by any valgrind tests. */
int fetch_object_timeout_handler(event_loop *loop, timer_id id, void *context) {
  LocalSchedulerState *state = (LocalSchedulerState *) context;
  /* Only try the fetches if we are connected to the object store manager. */
  if (!plasma_manager_is_connected(state->plasma_conn)) {
    LOG_INFO("Local scheduler is not connected to a object store manager");
    return LOCAL_SCHEDULER_FETCH_TIMEOUT_MILLISECONDS;
  }

  /* Allocate a buffer to hold all the object IDs for active fetch requests. */
  int num_object_ids = HASH_COUNT(state->algorithm_state->remote_objects);
  ObjectID *object_ids = (ObjectID *) malloc(num_object_ids * sizeof(ObjectID));

  /* Fill out the request with the object IDs for active fetches. */
  object_entry *fetch_request, *tmp;
  int i = 0;
  HASH_ITER(hh, state->algorithm_state->remote_objects, fetch_request, tmp) {
    object_ids[i] = fetch_request->object_id;
    ++i;
  }
  plasma_fetch(state->plasma_conn, num_object_ids, object_ids);
  for (int i = 0; i < num_object_ids; ++i) {
    reconstruct_object(state, object_ids[i]);
  }
  free(object_ids);
  return LOCAL_SCHEDULER_FETCH_TIMEOUT_MILLISECONDS;
}

/**
 * Assign as many tasks from the dispatch queue as possible.
 *
 * @param state The scheduler state.
 * @param algorithm_state The scheduling algorithm state.
 * @return Void.
 */
void dispatch_tasks(LocalSchedulerState *state,
                    SchedulingAlgorithmState *algorithm_state) {
  /* Assign as many tasks as we can, while there are workers available. */
  for (auto it = algorithm_state->dispatch_task_queue->begin();
       it != algorithm_state->dispatch_task_queue->end();) {
    TaskQueueEntry task = *it;
    /* If there is a task to assign, but there are no more available workers in
     * the worker pool, then exit. Ensure that there will be an available
     * worker during a future invocation of dispatch_tasks. */
    if (utarray_len(algorithm_state->available_workers) == 0) {
      if (utarray_len(state->child_pids) == 0) {
        /* If there are no workers, including those pending PID registration,
         * then we must start a new one to replenish the worker pool. */
        start_worker(state, NIL_ACTOR_ID);
      }
      return;
    }
    /* Terminate early if there are no more resources available. */
    bool resources_available = false;
    for (int i = 0; i < ResourceIndex_MAX; i++) {
      if (state->dynamic_resources[i] > 0) {
        /* There are still resources left, continue checking tasks. */
        resources_available = true;
        break;
      }
    }
    if (!resources_available) {
      /* No resources available -- terminate early. */
      return;
    }
    /* Skip to the next task if this task cannot currently be satisfied. */
    bool task_satisfied = true;
    for (int i = 0; i < ResourceIndex_MAX; i++) {
      if (TaskSpec_get_required_resource(task.spec, i) >
          state->dynamic_resources[i]) {
        /* Insufficient capacity for this task, proceed to the next task. */
        task_satisfied = false;
        break;
      }
    }
    if (!task_satisfied) {
      /* This task could not be satisfied -- proceed to the next task. */
      ++it;
      continue;
    }

    /* Dispatch this task to an available worker and dequeue the task. */
    LOG_DEBUG("Dispatching task");
    /* Get the last available worker in the available worker queue. */
    LocalSchedulerClient **worker = (LocalSchedulerClient **) utarray_back(
        algorithm_state->available_workers);
    /* Tell the available worker to execute the task. */
    assign_task_to_worker(state, task.spec, task.task_spec_size, *worker);
    /* Remove the worker from the available queue, and add it to the executing
     * workers. */
    utarray_pop_back(algorithm_state->available_workers);
    utarray_push_back(algorithm_state->executing_workers, worker);
    print_resource_info(state, task.spec);
    /* Free the task queue entry. */
    TaskQueueEntry_free(&task);
    /* Dequeue the task. */
    it = algorithm_state->dispatch_task_queue->erase(it);
  } /* End for each task in the dispatch queue. */
}

/**
 * A helper function to allocate a queue entry for a task specification and
 * push it onto a generic queue.
 *
 * @param state The state of the local scheduler.
 * @param task_queue A pointer to a task queue. NOTE: Because we are using
 *        utlist.h, we must pass in a pointer to the queue we want to append
 *        to. If we passed in the queue itself and the queue was empty, this
 *        would append the task to a queue that we don't have a reference to.
 * @param task_entry A pointer to the task entry to queue.
 * @param from_global_scheduler Whether or not the task was from a global
 *        scheduler. If false, the task was submitted by a worker.
 * @return A reference to the entry in the queue that was pushed.
 */
std::list<TaskQueueEntry>::iterator queue_task(
    LocalSchedulerState *state,
    std::list<TaskQueueEntry> *task_queue,
    TaskQueueEntry *task_entry,
    bool from_global_scheduler) {
  /* Copy the spec and add it to the task queue. The allocated spec will be
   * freed when it is assigned to a worker. */
  task_queue->push_back(*task_entry);
  /* Since we just queued the task, we can get a reference to it by going to
   * the last element in the queue. */
  auto it = state->algorithm_state->waiting_task_queue->end();
  --it;

  /* The task has been added to a local scheduler queue. Write the entry in the
   * task table to notify others that we have queued it. */
  if (state->db != NULL) {
    Task *task = Task_alloc(task_entry->spec, task_entry->task_spec_size,
                            TASK_STATUS_QUEUED, get_db_client_id(state->db));
    if (from_global_scheduler) {
      /* If the task is from the global scheduler, it's already been added to
       * the task table, so just update the entry. */
      task_table_update(state->db, task, NULL, NULL, NULL);
    } else {
      /* Otherwise, this is the first time the task has been seen in the system
       * (unless it's a resubmission of a previous task), so add the entry. */
      task_table_add_task(state->db, task, NULL, NULL, NULL);
    }
  }

  return it;
}

/**
 * Queue a task whose dependencies are missing. When the task's object
 * dependencies become available, the task will be moved to the dispatch queue.
 * If we have a connection to a plasma manager, begin trying to fetch the
 * dependencies.
 *
 * @param state The scheduler state.
 * @param algorithm_state The scheduling algorithm state.
 * @param spec The task specification to queue.
 * @param from_global_scheduler Whether or not the task was from a global
 *        scheduler. If false, the task was submitted by a worker.
 * @return Void.
 */
void queue_waiting_task(LocalSchedulerState *state,
                        SchedulingAlgorithmState *algorithm_state,
                        TaskSpec *spec,
                        int64_t task_spec_size,
                        bool from_global_scheduler) {
  LOG_DEBUG("Queueing task in waiting queue");
  TaskQueueEntry task_entry = TaskQueueEntry_init(spec, task_spec_size);
  auto it = queue_task(state, algorithm_state->waiting_task_queue, &task_entry,
                       from_global_scheduler);
  fetch_missing_dependencies(state, algorithm_state, it);
}

/**
 * Queue a task whose dependencies are ready. When the task reaches the front
 * of the dispatch queue and workers are available, it will be assigned.
 *
 * @param state The scheduler state.
 * @param algorithm_state The scheduling algorithm state.
 * @param spec The task specification to queue.
 * @param from_global_scheduler Whether or not the task was from a global
 *        scheduler. If false, the task was submitted by a worker.
 * @return Void.
 */
void queue_dispatch_task(LocalSchedulerState *state,
                         SchedulingAlgorithmState *algorithm_state,
                         TaskSpec *spec,
                         int64_t task_spec_size,
                         bool from_global_scheduler) {
  LOG_DEBUG("Queueing task in dispatch queue");
  TaskQueueEntry task_entry = TaskQueueEntry_init(spec, task_spec_size);
  queue_task(state, algorithm_state->dispatch_task_queue, &task_entry,
             from_global_scheduler);
}

/**
 * Add the task to the proper local scheduler queue. This assumes that the
 * scheduling decision to place the task on this node has already been made,
 * whether locally or by the global scheduler.
 *
 * @param state The scheduler state.
 * @param algorithm_state The scheduling algorithm state.
 * @param spec The task specification to queue.
 * @param from_global_scheduler Whether or not the task was from a global
 *        scheduler. If false, the task was submitted by a worker.
 * @return Void.
 */
void queue_task_locally(LocalSchedulerState *state,
                        SchedulingAlgorithmState *algorithm_state,
                        TaskSpec *spec,
                        int64_t task_spec_size,
                        bool from_global_scheduler) {
  if (can_run(algorithm_state, spec)) {
    /* Dependencies are ready, so push the task to the dispatch queue. */
    queue_dispatch_task(state, algorithm_state, spec, task_spec_size,
                        from_global_scheduler);
  } else {
    /* Dependencies are not ready, so push the task to the waiting queue. */
    queue_waiting_task(state, algorithm_state, spec, task_spec_size,
                       from_global_scheduler);
  }
}

/**
 * Give a task directly to another local scheduler. This is currently only used
 * for assigning actor tasks to the local scheduler responsible for that actor.
 *
 * @param state The scheduler state.
 * @param algorithm_state The scheduling algorithm state.
 * @param spec The task specification to schedule.
 * @param local_scheduler_id The ID of the local scheduler to give the task to.
 * @return Void.
 */
void give_task_to_local_scheduler(LocalSchedulerState *state,
                                  SchedulingAlgorithmState *algorithm_state,
                                  TaskSpec *spec,
                                  int64_t task_spec_size,
                                  DBClientID local_scheduler_id) {
  if (DBClientID_equal(local_scheduler_id, get_db_client_id(state->db))) {
    LOG_WARN("Local scheduler is trying to assign a task to itself.");
  }
  CHECK(state->db != NULL);
  /* Assign the task to the relevant local scheduler. */
  DCHECK(state->config.global_scheduler_exists);
  Task *task = Task_alloc(spec, task_spec_size, TASK_STATUS_SCHEDULED,
                          local_scheduler_id);
  task_table_add_task(state->db, task, NULL, NULL, NULL);
}

/**
 * Give a task to the global scheduler to schedule.
 *
 * @param state The scheduler state.
 * @param algorithm_state The scheduling algorithm state.
 * @param spec The task specification to schedule.
 * @return Void.
 */
void give_task_to_global_scheduler(LocalSchedulerState *state,
                                   SchedulingAlgorithmState *algorithm_state,
                                   TaskSpec *spec,
                                   int64_t task_spec_size) {
  if (state->db == NULL || !state->config.global_scheduler_exists) {
    /* A global scheduler is not available, so queue the task locally. */
    queue_task_locally(state, algorithm_state, spec, task_spec_size, false);
    return;
  }
  /* Pass on the task to the global scheduler. */
  DCHECK(state->config.global_scheduler_exists);
  Task *task = Task_alloc(spec, task_spec_size, TASK_STATUS_WAITING, NIL_ID);
  DCHECK(state->db != NULL);
  task_table_add_task(state->db, task, NULL, NULL, NULL);
}

bool resource_constraints_satisfied(LocalSchedulerState *state,
                                    TaskSpec *spec) {
  /* At the local scheduler, if required resource vector exceeds either static
   * or dynamic resource vector, the resource constraint is not satisfied. */
  for (int i = 0; i < ResourceIndex_MAX; i++) {
    if (TaskSpec_get_required_resource(spec, i) > state->static_resources[i] ||
        TaskSpec_get_required_resource(spec, i) > state->dynamic_resources[i]) {
      return false;
    }
  }
  return true;
}

void handle_task_submitted(LocalSchedulerState *state,
                           SchedulingAlgorithmState *algorithm_state,
                           TaskSpec *spec,
                           int64_t task_spec_size) {
  /* TODO(atumanov): if static is satisfied and local objects ready, but dynamic
   * resource is currently unavailable, then consider queueing task locally and
   * recheck dynamic next time. */

  /* If this task's constraints are satisfied, dependencies are available
   * locally, and there is an available worker, then enqueue the task in the
   * dispatch queue and trigger task dispatch. Otherwise, pass the task along to
   * the global scheduler if there is one. */
  if (resource_constraints_satisfied(state, spec) &&
      (utarray_len(algorithm_state->available_workers) > 0) &&
      can_run(algorithm_state, spec)) {
    queue_dispatch_task(state, algorithm_state, spec, task_spec_size, false);
  } else {
    /* Give the task to the global scheduler to schedule, if it exists. */
    give_task_to_global_scheduler(state, algorithm_state, spec, task_spec_size);
  }

  /* Try to dispatch tasks, since we may have added one to the queue. */
  dispatch_tasks(state, algorithm_state);
}

void handle_actor_task_submitted(LocalSchedulerState *state,
                                 SchedulingAlgorithmState *algorithm_state,
                                 TaskSpec *spec,
                                 int64_t task_spec_size) {
  ActorID actor_id = TaskSpec_actor_id(spec);
  CHECK(!ActorID_equal(actor_id, NIL_ACTOR_ID));

  /* Find the local scheduler responsible for this actor. */
  actor_map_entry *entry;
  HASH_FIND(hh, state->actor_mapping, &actor_id, sizeof(actor_id), entry);

  if (entry == NULL) {
    /* Add this task to a queue of tasks that have been submitted but the local
     * scheduler doesn't know which actor is responsible for them. These tasks
     * will be resubmitted (internally by the local scheduler) whenever a new
     * actor notification arrives. */
    utarray_push_back(algorithm_state->cached_submitted_actor_tasks, &spec);
    utarray_push_back(algorithm_state->cached_submitted_actor_task_sizes,
                      &task_spec_size);
    return;
  }

  if (DBClientID_equal(entry->local_scheduler_id,
                       get_db_client_id(state->db))) {
    /* This local scheduler is responsible for the actor, so handle the task
     * locally. */
    add_task_to_actor_queue(state, algorithm_state, spec, task_spec_size,
                            false);
    /* Attempt to dispatch tasks to this actor. */
    dispatch_actor_task(state, algorithm_state, actor_id);
  } else {
    /* This local scheduler is not responsible for the task, so assign the task
     * directly to the actor that is responsible. */
    give_task_to_local_scheduler(state, algorithm_state, spec, task_spec_size,
                                 entry->local_scheduler_id);
  }
}

void handle_actor_creation_notification(
    LocalSchedulerState *state,
    SchedulingAlgorithmState *algorithm_state,
    ActorID actor_id) {
  int num_cached_actor_tasks =
      utarray_len(algorithm_state->cached_submitted_actor_tasks);
  CHECK(num_cached_actor_tasks ==
        utarray_len(algorithm_state->cached_submitted_actor_task_sizes));
  for (int i = 0; i < num_cached_actor_tasks; ++i) {
    TaskSpec **spec = (TaskSpec **) utarray_eltptr(
        algorithm_state->cached_submitted_actor_tasks, i);
    int64_t *task_spec_size = (int64_t *) utarray_eltptr(
        algorithm_state->cached_submitted_actor_task_sizes, i);
    /* Note that handle_actor_task_submitted may append the spec to the end of
     * the cached_submitted_actor_tasks array. */
    handle_actor_task_submitted(state, algorithm_state, *spec, *task_spec_size);
  }
  /* Remove all the tasks that were resubmitted. This does not erase the tasks
   * that were newly appended to the cached_submitted_actor_tasks array. */
  utarray_erase(algorithm_state->cached_submitted_actor_tasks, 0,
                num_cached_actor_tasks);
  utarray_erase(algorithm_state->cached_submitted_actor_task_sizes, 0,
                num_cached_actor_tasks);
}

void handle_task_scheduled(LocalSchedulerState *state,
                           SchedulingAlgorithmState *algorithm_state,
                           TaskSpec *spec,
                           int64_t task_spec_size) {
  /* This callback handles tasks that were assigned to this local scheduler by
   * the global scheduler, so we can safely assert that there is a connection to
   * the database. */
  DCHECK(state->db != NULL);
  DCHECK(state->config.global_scheduler_exists);
  /* Push the task to the appropriate queue. */
  queue_task_locally(state, algorithm_state, spec, task_spec_size, true);
  dispatch_tasks(state, algorithm_state);
}

void handle_actor_task_scheduled(LocalSchedulerState *state,
                                 SchedulingAlgorithmState *algorithm_state,
                                 TaskSpec *spec,
                                 int64_t task_spec_size) {
  /* This callback handles tasks that were assigned to this local scheduler by
   * the global scheduler or by other workers, so we can safely assert that
   * there is a connection to the database. */
  DCHECK(state->db != NULL);
  DCHECK(state->config.global_scheduler_exists);
  /* Check that the task is meant to run on an actor that this local scheduler
   * is responsible for. */
  ActorID actor_id = TaskSpec_actor_id(spec);
  DCHECK(!ActorID_equal(actor_id, NIL_ACTOR_ID));
  actor_map_entry *entry;
  HASH_FIND(hh, state->actor_mapping, &actor_id, sizeof(actor_id), entry);
  if (entry != NULL) {
    /* This means that an actor has been assigned to this local scheduler, and a
     * task for that actor has been received by this local scheduler, but this
     * local scheduler has not yet processed the notification about the actor
     * creation. This may be possible though should be very uncommon. If it does
     * happen, it's ok. */
    DCHECK(DBClientID_equal(entry->local_scheduler_id,
                            get_db_client_id(state->db)));
  } else {
    LOG_INFO(
        "handle_actor_task_scheduled called on local scheduler but the "
        "corresponding actor_map_entry is not present. This should be rare.");
  }
  /* Push the task to the appropriate queue. */
  add_task_to_actor_queue(state, algorithm_state, spec, task_spec_size, true);
  dispatch_actor_task(state, algorithm_state, actor_id);
}

void handle_worker_available(LocalSchedulerState *state,
                             SchedulingAlgorithmState *algorithm_state,
                             LocalSchedulerClient *worker) {
  CHECK(worker->task_in_progress == NULL);
  /* Check that the worker isn't in the pool of available workers. */
  for (LocalSchedulerClient **p = (LocalSchedulerClient **) utarray_front(
           algorithm_state->available_workers);
       p != NULL; p = (LocalSchedulerClient **) utarray_next(
                      algorithm_state->available_workers, p)) {
    DCHECK(*p != worker);
  }
  /* Check that the worker isn't in the list of blocked workers. */
  for (LocalSchedulerClient **p = (LocalSchedulerClient **) utarray_front(
           algorithm_state->blocked_workers);
       p != NULL; p = (LocalSchedulerClient **) utarray_next(
                      algorithm_state->blocked_workers, p)) {
    DCHECK(*p != worker);
  }
  /* If the worker was executing a task, it must have finished, so remove it
   * from the list of executing workers. */
  for (int i = 0; i < utarray_len(algorithm_state->executing_workers); ++i) {
    LocalSchedulerClient **p = (LocalSchedulerClient **) utarray_eltptr(
        algorithm_state->executing_workers, i);
    if (*p == worker) {
      utarray_erase(algorithm_state->executing_workers, i, 1);
      break;
    }
  }
  /* Check that we actually erased the worker. */
  for (int i = 0; i < utarray_len(algorithm_state->executing_workers); ++i) {
    LocalSchedulerClient **p = (LocalSchedulerClient **) utarray_eltptr(
        algorithm_state->executing_workers, i);
    DCHECK(*p != worker);
  }

  /* Add worker to the list of available workers. */
  utarray_push_back(algorithm_state->available_workers, &worker);

  /* Try to dispatch tasks, since we now have available workers to assign them
   * to. */
  dispatch_tasks(state, algorithm_state);
}

void handle_worker_removed(LocalSchedulerState *state,
                           SchedulingAlgorithmState *algorithm_state,
                           LocalSchedulerClient *worker) {
  /* Make sure that we remove the worker at most once. */
  bool removed = false;
  int64_t num_workers;

  /* Remove the worker from available workers, if it's there. */
  num_workers = utarray_len(algorithm_state->available_workers);
  for (int64_t i = num_workers - 1; i >= 0; --i) {
    LocalSchedulerClient **p = (LocalSchedulerClient **) utarray_eltptr(
        algorithm_state->available_workers, i);
    DCHECK(!((*p == worker) && removed));
    if (*p == worker) {
      utarray_erase(algorithm_state->available_workers, i, 1);
      removed = true;
    }
  }

  /* Remove the worker from executing workers, if it's there. */
  num_workers = utarray_len(algorithm_state->executing_workers);
  for (int64_t i = num_workers - 1; i >= 0; --i) {
    LocalSchedulerClient **p = (LocalSchedulerClient **) utarray_eltptr(
        algorithm_state->executing_workers, i);
    DCHECK(!((*p == worker) && removed));
    if (*p == worker) {
      utarray_erase(algorithm_state->executing_workers, i, 1);
      removed = true;
    }
  }

  /* Remove the worker from blocked workers, if it's there. */
  num_workers = utarray_len(algorithm_state->blocked_workers);
  for (int64_t i = num_workers - 1; i >= 0; --i) {
    LocalSchedulerClient **p = (LocalSchedulerClient **) utarray_eltptr(
        algorithm_state->blocked_workers, i);
    DCHECK(!((*p == worker) && removed));
    if (*p == worker) {
      utarray_erase(algorithm_state->blocked_workers, i, 1);
      removed = true;
    }
  }
}

void handle_actor_worker_available(LocalSchedulerState *state,
                                   SchedulingAlgorithmState *algorithm_state,
                                   LocalSchedulerClient *worker) {
  ActorID actor_id = worker->actor_id;
  CHECK(!ActorID_equal(actor_id, NIL_ACTOR_ID));
  /* Get the actor info for this worker. */
  LocalActorInfo *entry;
  HASH_FIND(hh, algorithm_state->local_actor_infos, &actor_id, sizeof(actor_id),
            entry);
  CHECK(entry != NULL);
  CHECK(worker == entry->worker);
  CHECK(!entry->worker_available);
  entry->worker_available = true;
  /* Assign a task to this actor if possible. */
  dispatch_actor_task(state, algorithm_state, actor_id);
}

void handle_worker_blocked(LocalSchedulerState *state,
                           SchedulingAlgorithmState *algorithm_state,
                           LocalSchedulerClient *worker) {
  /* Find the worker in the list of executing workers. */
  for (int i = 0; i < utarray_len(algorithm_state->executing_workers); ++i) {
    LocalSchedulerClient **p = (LocalSchedulerClient **) utarray_eltptr(
        algorithm_state->executing_workers, i);
    if (*p == worker) {
      /* Remove the worker from the list of executing workers. */
      utarray_erase(algorithm_state->executing_workers, i, 1);

      /* Check that the worker isn't in the list of blocked workers. */
      for (LocalSchedulerClient **q = (LocalSchedulerClient **) utarray_front(
               algorithm_state->blocked_workers);
           q != NULL; q = (LocalSchedulerClient **) utarray_next(
                          algorithm_state->blocked_workers, q)) {
        DCHECK(*q != worker);
      }

      /* Add the worker to the list of blocked workers. */
      worker->is_blocked = true;
      utarray_push_back(algorithm_state->blocked_workers, &worker);
      /* Return the resources that the blocked worker was using. */
      CHECK(worker->task_in_progress != NULL);
      TaskSpec *spec = Task_task_spec(worker->task_in_progress);
      update_dynamic_resources(state, spec, true);

      /* Try to dispatch tasks, since we may have freed up some resources. */
      dispatch_tasks(state, algorithm_state);
      return;
    }
  }

  /* The worker should have been in the list of executing workers, so this line
   * should be unreachable. */
  LOG_FATAL(
      "Worker registered as blocked, but it was not in the list of executing "
      "workers.");
}

void handle_worker_unblocked(LocalSchedulerState *state,
                             SchedulingAlgorithmState *algorithm_state,
                             LocalSchedulerClient *worker) {
  /* Find the worker in the list of blocked workers. */
  for (int i = 0; i < utarray_len(algorithm_state->blocked_workers); ++i) {
    LocalSchedulerClient **p = (LocalSchedulerClient **) utarray_eltptr(
        algorithm_state->blocked_workers, i);
    if (*p == worker) {
      /* Remove the worker from the list of blocked workers. */
      utarray_erase(algorithm_state->blocked_workers, i, 1);

      /* Check that the worker isn't in the list of executing workers. */
      for (LocalSchedulerClient **q = (LocalSchedulerClient **) utarray_front(
               algorithm_state->executing_workers);
           q != NULL; q = (LocalSchedulerClient **) utarray_next(
                          algorithm_state->executing_workers, q)) {
        DCHECK(*q != worker);
      }

      /* Lease back the resources that the blocked worker will need. */
      /* TODO(swang): Leasing back the resources to blocked workers can cause
       * us to transiently exceed the maximum number of resources. This can be
       * fixed by having blocked workers explicitly yield and wait to be given
       * back resources before continuing execution. */
      CHECK(worker->task_in_progress != NULL);
      TaskSpec *spec = Task_task_spec(worker->task_in_progress);
      update_dynamic_resources(state, spec, false);
      /* Add the worker to the list of executing workers. */
      worker->is_blocked = false;
      utarray_push_back(algorithm_state->executing_workers, &worker);

      return;
    }
  }

  /* The worker should have been in the list of blocked workers, so this line
   * should be unreachable. */
  LOG_FATAL(
      "Worker registered as unblocked, but it was not in the list of blocked "
      "workers.");
}

void handle_object_available(LocalSchedulerState *state,
                             SchedulingAlgorithmState *algorithm_state,
                             ObjectID object_id) {
  /* Get the entry for this object from the active fetch request, or allocate
   * one if needed. */
  object_entry *entry;
  HASH_FIND(hh, algorithm_state->remote_objects, &object_id, sizeof(object_id),
            entry);
  if (entry != NULL) {
    /* Remove the object from the active fetch requests. */
    HASH_DELETE(hh, algorithm_state->remote_objects, entry);
  } else {
    /* Allocate a new object entry. Object entries will get freed if the object
     * is removed. */
    entry = (object_entry *) malloc(sizeof(object_entry));
    entry->object_id = object_id;
    entry->dependent_tasks =
        new std::vector<std::list<TaskQueueEntry>::iterator>();
  }

  /* Add the entry to the set of locally available objects. */
  HASH_ADD(hh, algorithm_state->local_objects, object_id, sizeof(object_id),
           entry);

  if (!entry->dependent_tasks->empty()) {
    /* Out of the tasks that were dependent on this object, if they are now
     * ready to run, move them to the dispatch queue. */
    for (auto &it : *entry->dependent_tasks) {
      if (can_run(algorithm_state, it->spec)) {
        LOG_DEBUG("Moved task to dispatch queue");
        algorithm_state->dispatch_task_queue->push_back(*it);
        /* Remove the entry with a matching TaskSpec pointer from the waiting
         * queue, but do not free the task spec. */
        algorithm_state->waiting_task_queue->erase(it);
      }
    }
    /* Try to dispatch tasks, since we may have added some from the waiting
     * queue. */
    dispatch_tasks(state, algorithm_state);
    /* Clean up the records for dependent tasks. */
    entry->dependent_tasks->clear();
  }
}

void handle_object_removed(LocalSchedulerState *state,
                           ObjectID removed_object_id) {
  /* Remove the object from the set of locally available objects. */
  SchedulingAlgorithmState *algorithm_state = state->algorithm_state;
  object_entry *entry;
  HASH_FIND(hh, algorithm_state->local_objects, &removed_object_id,
            sizeof(removed_object_id), entry);
  CHECK(entry != NULL);
  HASH_DELETE(hh, algorithm_state->local_objects, entry);
  delete entry->dependent_tasks;
  free(entry);

  /* Track queued tasks that were dependent on this object.
   * NOTE: Since objects often get removed in batches (e.g., during eviction),
   * we may end up iterating through the queues many times in a row. If this
   * turns out to be a bottleneck, consider tracking dependencies even for
   * tasks in the dispatch queue, or batching object notifications. */
  /* Track the dependency for tasks that were in the dispatch queue. Remove
   * these tasks from the dispatch queue and push them to the waiting queue. */
  for (auto it = algorithm_state->dispatch_task_queue->begin();
       it != algorithm_state->dispatch_task_queue->end();) {
    TaskQueueEntry task = *it;
    if (TaskSpec_is_dependent_on(task.spec, removed_object_id)) {
      /* This task was dependent on the removed object. */
      LOG_DEBUG("Moved task from dispatch queue back to waiting queue");
      algorithm_state->waiting_task_queue->push_back(task);
      /* Remove the task from the dispatch queue, but do not free the task
       * spec. */
      it = algorithm_state->dispatch_task_queue->erase(it);
    } else {
      /* The task can still run, so continue to the next task. */
      ++it;
    }
  }

  /* Track the dependency for tasks that are in the waiting queue, including
   * those that were just moved from the dispatch queue. */
  for (auto it = algorithm_state->waiting_task_queue->begin();
       it != algorithm_state->waiting_task_queue->end(); ++it) {
    int64_t num_args = TaskSpec_num_args(it->spec);
    for (int i = 0; i < num_args; ++i) {
      if (TaskSpec_arg_by_ref(it->spec, i)) {
        ObjectID arg_id = TaskSpec_arg_id(it->spec, i);
        if (ObjectID_equal(arg_id, removed_object_id)) {
          fetch_missing_dependency(state, algorithm_state, it,
                                   removed_object_id);
        }
      }
    }
  }
}

int num_waiting_tasks(SchedulingAlgorithmState *algorithm_state) {
  return algorithm_state->waiting_task_queue->size();
}

int num_dispatch_tasks(SchedulingAlgorithmState *algorithm_state) {
  return algorithm_state->dispatch_task_queue->size();
}

void print_worker_info(const char *message,
                       SchedulingAlgorithmState *algorithm_state) {
  LOG_DEBUG("%s: %d available, %d executing, %d blocked", message,
            utarray_len(algorithm_state->available_workers),
            utarray_len(algorithm_state->executing_workers),
            utarray_len(algorithm_state->blocked_workers));
}
