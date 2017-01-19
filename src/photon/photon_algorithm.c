#include "photon_algorithm.h"

#include <stdbool.h>
#include "utarray.h"
#include "utlist.h"

#include "state/task_table.h"
#include "state/local_scheduler_table.h"
#include "state/object_table.h"
#include "photon.h"
#include "photon_scheduler.h"

typedef struct task_queue_entry {
  /** The task that is queued. */
  task_spec *spec;
  struct task_queue_entry *prev;
  struct task_queue_entry *next;
} task_queue_entry;

typedef struct {
  /** Object id of this object. */
  object_id object_id;
  /** Handle for the uthash table. */
  UT_hash_handle handle;
} available_object;

/** A data structure used to track which objects are being fetched. */
typedef struct {
  /** The object ID that we are trying to fetch. */
  object_id object_id;
  /** The local scheduler state. */
  local_scheduler_state *state;
  /** The scheduling algorithm state. */
  scheduling_algorithm_state *algorithm_state;
  /** The ID for the timer that will time out the current request. */
  int64_t timer;
  /** Handle for the uthash table. */
  UT_hash_handle hh;
} fetch_object_request;

/** Part of the photon state that is maintained by the scheduling algorithm. */
struct scheduling_algorithm_state {
  /** An array of pointers to tasks that are waiting for dependencies. */
  task_queue_entry *waiting_task_queue;
  /** An array of pointers to tasks whose dependencies are ready but that are
   *  waiting to be assigned to a worker. */
  task_queue_entry *dispatch_task_queue;
  /** An array of worker indices corresponding to clients that are
   *  waiting for tasks. */
  UT_array *available_workers;
  /** A hash map of the objects that are available in the local Plasma store.
   *  This information could be a little stale. */
  available_object *local_objects;
  /** A hash map of the objects that are currently being fetched by this local
   *  scheduler. The key is the object ID. */
  fetch_object_request *fetch_requests;
};

scheduling_algorithm_state *make_scheduling_algorithm_state(void) {
  scheduling_algorithm_state *algorithm_state =
      malloc(sizeof(scheduling_algorithm_state));
  /* Initialize an empty hash map for the cache of local available objects. */
  algorithm_state->local_objects = NULL;
  /* Initialize the local data structures used for queuing tasks and workers. */
  algorithm_state->waiting_task_queue = NULL;
  algorithm_state->dispatch_task_queue = NULL;
  utarray_new(algorithm_state->available_workers, &ut_int_icd);
  /* Initialize the hash table of objects being fetched. */
  algorithm_state->fetch_requests = NULL;
  return algorithm_state;
}

void free_scheduling_algorithm_state(
    scheduling_algorithm_state *algorithm_state) {
  task_queue_entry *elt, *tmp1;
  DL_FOREACH_SAFE(algorithm_state->waiting_task_queue, elt, tmp1) {
    DL_DELETE(algorithm_state->waiting_task_queue, elt);
    free_task_spec(elt->spec);
    free(elt);
  }
  DL_FOREACH_SAFE(algorithm_state->dispatch_task_queue, elt, tmp1) {
    DL_DELETE(algorithm_state->dispatch_task_queue, elt);
    free_task_spec(elt->spec);
    free(elt);
  }
  utarray_free(algorithm_state->available_workers);
  available_object *available_obj, *tmp2;
  HASH_ITER(handle, algorithm_state->local_objects, available_obj, tmp2) {
    HASH_DELETE(handle, algorithm_state->local_objects, available_obj);
    free(available_obj);
  }
  fetch_object_request *fetch_elt, *tmp_fetch_elt;
  HASH_ITER(hh, algorithm_state->fetch_requests, fetch_elt, tmp_fetch_elt) {
    HASH_DELETE(hh, algorithm_state->fetch_requests, fetch_elt);
    free(fetch_elt);
  }
  free(algorithm_state);
}

void provide_scheduler_info(local_scheduler_state *state,
                            scheduling_algorithm_state *algorithm_state,
                            local_scheduler_info *info) {
  task_queue_entry *elt;
  info->total_num_workers = utarray_len(state->workers);
  /* TODO(swang): Provide separate counts for tasks that are waiting for
   * dependencies vs tasks that are waiting to be assigned. */
  int waiting_task_queue_length;
  DL_COUNT(algorithm_state->waiting_task_queue, elt, waiting_task_queue_length);
  int dispatch_task_queue_length;
  DL_COUNT(algorithm_state->dispatch_task_queue, elt,
           dispatch_task_queue_length);
  info->task_queue_length =
      waiting_task_queue_length + dispatch_task_queue_length;
  info->available_workers = utarray_len(algorithm_state->available_workers);
}

/**
 * Check if all of the remote object arguments for a task are available in the
 * local object store.
 *
 * @param s The scheduler state.
 * @param task Task specification of the task to check.
 * @return This returns 1 if all of the remote object arguments for the task are
 *         present in the local object store, otherwise it returns 0.
 */
bool can_run(scheduling_algorithm_state *algorithm_state, task_spec *task) {
  int64_t num_args = task_num_args(task);
  for (int i = 0; i < num_args; ++i) {
    if (task_arg_type(task, i) == ARG_BY_REF) {
      object_id obj_id = task_arg_id(task, i);
      available_object *entry;
      HASH_FIND(handle, algorithm_state->local_objects, &obj_id, sizeof(obj_id),
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

/* TODO(rkn): This method will need to be changed to call reconstruct. */
int fetch_object_timeout_handler(event_loop *loop, timer_id id, void *context) {
  fetch_object_request *fetch_req = (fetch_object_request *) context;
  object_id object_ids[1] = {fetch_req->object_id};
  plasma_fetch(fetch_req->state->plasma_conn, 1, object_ids);
  return LOCAL_SCHEDULER_FETCH_TIMEOUT_MILLISECONDS;
}

void fetch_missing_dependencies(local_scheduler_state *state,
                                scheduling_algorithm_state *algorithm_state,
                                task_spec *spec) {
  int64_t num_args = task_num_args(spec);
  for (int i = 0; i < num_args; ++i) {
    if (task_arg_type(spec, i) == ARG_BY_REF) {
      object_id obj_id = task_arg_id(spec, i);
      available_object *entry;
      HASH_FIND(handle, algorithm_state->local_objects, &obj_id, sizeof(obj_id),
                entry);
      if (entry == NULL) {
        /* The object is not present locally, fetch the object. */
        object_id object_ids[1] = {obj_id};
        plasma_fetch(state->plasma_conn, 1, object_ids);
        /* Create a fetch request and add a timer to the event loop to ensure
         * that the fetch actually happens. */
        fetch_object_request *fetch_req = malloc(sizeof(fetch_object_request));
        fetch_req->object_id = obj_id;
        fetch_req->state = state;
        fetch_req->algorithm_state = algorithm_state;
        fetch_req->timer = event_loop_add_timer(
            state->loop, LOCAL_SCHEDULER_FETCH_TIMEOUT_MILLISECONDS,
            fetch_object_timeout_handler, fetch_req);
        /* The fetch request will be freed and removed from the hash table in
         * handle_object_available when the object becomes available locally. */
        HASH_ADD(hh, algorithm_state->fetch_requests, object_id,
                 sizeof(fetch_req->object_id), fetch_req);
      }
    }
  }
}

/**
 * Assign as many tasks from the dispatch queue as possible.
 *
 * @param state The scheduler state.
 * @param algorithm_state The scheduling algorithm state.
 * @return Void.
 */
void dispatch_tasks(local_scheduler_state *state,
                    scheduling_algorithm_state *algorithm_state) {
  /* Assign tasks while there are still tasks in the dispatch queue and
   * available workers. */
  while ((algorithm_state->dispatch_task_queue != NULL) &&
         (utarray_len(algorithm_state->available_workers) > 0)) {
    LOG_DEBUG("Dispatching task");
    /* Pop a task from the dispatch queue. */
    task_queue_entry *dispatched_task = algorithm_state->dispatch_task_queue;
    DL_DELETE(algorithm_state->dispatch_task_queue, dispatched_task);

    /* Get the last available worker in the available worker queue. */
    int *worker_index =
        (int *) utarray_back(algorithm_state->available_workers);
    /* Tell the available worker to execute the task. */
    assign_task_to_worker(state, dispatched_task->spec, *worker_index);
    /* Remove the available worker from the queue and free the struct. */
    utarray_pop_back(algorithm_state->available_workers);
    free_task_spec(dispatched_task->spec);
    free(dispatched_task);
  }
}

/**
 * A helper function to allocate a queue entry for a task specification and
 * push it onto a generic queue.
 *
 * @param task_queue A pointer to a task queue. NOTE: Because we are using
 *        utlist.h, we must pass in a pointer to the queue we want to append
 *        to. If we passed in the queue itself and the queue was empty, this
 *        would append the task to a queue that we don't have a reference to.
 * @param spec The task specification to queue.
 * @return Void.
 */
void queue_task(local_scheduler_state *state,
                task_queue_entry **task_queue,
                task_spec *spec) {
  /* Copy the spec and add it to the task queue. The allocated spec will be
   * freed when it is assigned to a worker. */
  task_queue_entry *elt = malloc(sizeof(task_queue_entry));
  elt->spec = (task_spec *) malloc(task_spec_size(spec));
  memcpy(elt->spec, spec, task_spec_size(spec));
  DL_APPEND((*task_queue), elt);

  /* The task has been added to a local scheduler queue. Add the task to the
   * task table to notify others that we have queued it. */
  if (state->db != NULL) {
    task *task =
        alloc_task(spec, TASK_STATUS_QUEUED, get_db_client_id(state->db));
    task_table_add_task(state->db, task, (retry_info *) &photon_retry, NULL,
                        NULL);
  }
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
 * @return Void.
 */
void queue_waiting_task(local_scheduler_state *state,
                        scheduling_algorithm_state *algorithm_state,
                        task_spec *spec) {
  LOG_DEBUG("Queueing task in waiting queue");
  /* Initiate fetch calls for any dependencies that are not present locally. */
  if (plasma_manager_is_connected(state->plasma_conn)) {
    fetch_missing_dependencies(state, algorithm_state, spec);
  }
  queue_task(state, &algorithm_state->waiting_task_queue, spec);
}

/**
 * Queue a task whose dependencies are ready. When the task reaches the front
 * of the dispatch queue and workers are available, it will be assigned.
 *
 * @param state The scheduler state.
 * @param algorithm_state The scheduling algorithm state.
 * @param spec The task specification to queue.
 * @return Void.
 */
void queue_dispatch_task(local_scheduler_state *state,
                         scheduling_algorithm_state *algorithm_state,
                         task_spec *spec) {
  LOG_DEBUG("Queueing task in dispatch queue");
  queue_task(state, &algorithm_state->dispatch_task_queue, spec);
}

/**
 * Add the task to the proper local scheduler queue. This assumes that the
 * scheduling decision to place the task on this node has already been made,
 * whether locally or by the global scheduler.
 *
 * @param state The scheduler state.
 * @param algorithm_state The scheduling algorithm state.
 * @param spec The task specification to queue.
 * @return Void.
 */
void queue_task_locally(local_scheduler_state *state,
                        scheduling_algorithm_state *algorithm_state,
                        task_spec *spec) {
  if (can_run(algorithm_state, spec)) {
    /* Dependencies are ready, so push the task to the dispatch queue. */
    queue_dispatch_task(state, algorithm_state, spec);
  } else {
    /* Dependencies are not ready, so push the task to the waiting queue. */
    queue_waiting_task(state, algorithm_state, spec);
  }
}

/**
 * Give a task to the global scheduler to schedule.
 *
 * @param state The scheduler state.
 * @param algorithm_state The scheduling algorithm state.
 * @param spec The task specification to schedule.
 * @return Void.
 */
void give_task_to_global_scheduler(local_scheduler_state *state,
                                   scheduling_algorithm_state *algorithm_state,
                                   task_spec *spec) {
  if (state->db == NULL || !state->global_scheduler_exists) {
    /* A global scheduler is not available, so queue the task locally. */
    queue_task_locally(state, algorithm_state, spec);
    return;
  }
  /* Pass on the task to the global scheduler. */
  DCHECK(state->global_scheduler_exists);
  task *task = alloc_task(spec, TASK_STATUS_WAITING, NIL_ID);
  DCHECK(state->db != NULL);
  task_table_add_task(state->db, task, (retry_info *) &photon_retry, NULL,
                      NULL);
}

void handle_task_submitted(local_scheduler_state *state,
                           scheduling_algorithm_state *algorithm_state,
                           task_spec *spec) {
  /* If this task's dependencies are available locally, and if there is an
   * available worker, then assign this task to an available worker. If we
   * cannot assign the task to a worker immediately, we either queue the task in
   * the local task queue or we pass the task to the global scheduler. For now,
   * we pass the task along to the global scheduler if there is one. */
  if (can_run(algorithm_state, spec) &&
      (utarray_len(algorithm_state->available_workers) > 0)) {
    /* Dependencies are ready and there is an available worker, so dispatch the
     * task. */
    queue_dispatch_task(state, algorithm_state, spec);
  } else {
    /* Give the task to the global scheduler to schedule, if it exists. */
    give_task_to_global_scheduler(state, algorithm_state, spec);
  }

  /* Try to dispatch tasks, since we may have added one to the queue. */
  dispatch_tasks(state, algorithm_state);

  /* Update the result table, which holds mappings of object ID -> ID of the
   * task that created it. */
  if (state->db != NULL) {
    task_id task_id = task_spec_id(spec);
    for (int64_t i = 0; i < task_num_returns(spec); ++i) {
      object_id return_id = task_return(spec, i);
      result_table_add(state->db, return_id, task_id,
                       (retry_info *) &photon_retry, NULL, NULL);
    }
  }
}

void handle_task_scheduled(local_scheduler_state *state,
                           scheduling_algorithm_state *algorithm_state,
                           task_spec *spec) {
  /* This callback handles tasks that were assigned to this local scheduler by
   * the global scheduler, so we can safely assert that there is a connection
   * to the database. */
  DCHECK(state->db != NULL);
  DCHECK(state->global_scheduler_exists);
  /* Push the task to the appropriate queue. */
  queue_task_locally(state, algorithm_state, spec);
  dispatch_tasks(state, algorithm_state);
}

void handle_worker_available(local_scheduler_state *state,
                             scheduling_algorithm_state *algorithm_state,
                             int worker_index) {
  worker *available_worker =
      (worker *) utarray_eltptr(state->workers, worker_index);
  CHECK(available_worker->task_in_progress == NULL);
  for (int *p = (int *) utarray_front(algorithm_state->available_workers);
       p != NULL;
       p = (int *) utarray_next(algorithm_state->available_workers, p)) {
    DCHECK(*p != worker_index);
  }
  /* Add worker to the list of available workers. */
  utarray_push_back(algorithm_state->available_workers, &worker_index);
  LOG_DEBUG("Adding worker_index %d to available workers.\n", worker_index);

  /* Try to dispatch tasks, since we now have available workers to assign them
   * to. */
  dispatch_tasks(state, algorithm_state);
}

void handle_object_available(local_scheduler_state *state,
                             scheduling_algorithm_state *algorithm_state,
                             object_id object_id) {
  /* Available object entries get freed if the object is removed. */
  available_object *entry =
      (available_object *) malloc(sizeof(available_object));
  entry->object_id = object_id;
  HASH_ADD(handle, algorithm_state->local_objects, object_id, sizeof(object_id),
           entry);

  /* If we were previously trying to fetch this object, remove the fetch request
   * from the hash table. */
  fetch_object_request *fetch_req;
  HASH_FIND(hh, algorithm_state->fetch_requests, &object_id, sizeof(object_id),
            fetch_req);
  if (fetch_req != NULL) {
    HASH_DELETE(hh, algorithm_state->fetch_requests, fetch_req);
    CHECK(event_loop_remove_timer(state->loop, fetch_req->timer) == AE_OK);
    free(fetch_req);
  }

  /* Move any tasks whose object dependencies are now ready to the dispatch
   * queue. */
  /* TODO(swang): This can be optimized by keeping a lookup table from object
   * ID to list of dependent tasks in the waiting queue. */
  task_queue_entry *elt, *tmp;
  DL_FOREACH_SAFE(algorithm_state->waiting_task_queue, elt, tmp) {
    if (can_run(algorithm_state, elt->spec)) {
      LOG_DEBUG("Moved task to dispatch queue");
      DL_DELETE(algorithm_state->waiting_task_queue, elt);
      DL_APPEND(algorithm_state->dispatch_task_queue, elt);
    }
  }

  /* Try to dispatch tasks, since we may have added some from the waiting
   * queue. */
  dispatch_tasks(state, algorithm_state);
}

void handle_object_removed(local_scheduler_state *state, object_id object_id) {
  /* TODO(swang): Move dependent tasks from the dispatch queue back to the
   * waiting queue. It's okay not to do this since the worker will get any
   * missing objects, but this may lead to unnecessarily blocked workers. */
  scheduling_algorithm_state *algorithm_state = state->algorithm_state;
  available_object *entry;
  HASH_FIND(handle, algorithm_state->local_objects, &object_id,
            sizeof(object_id), entry);
  if (entry != NULL) {
    HASH_DELETE(handle, algorithm_state->local_objects, entry);
    free(entry);
  }
}

int num_waiting_tasks(scheduling_algorithm_state *algorithm_state) {
  task_queue_entry *elt;
  int count;
  DL_COUNT(algorithm_state->waiting_task_queue, elt, count);
  return count;
}

int num_dispatch_tasks(scheduling_algorithm_state *algorithm_state) {
  task_queue_entry *elt;
  int count;
  DL_COUNT(algorithm_state->dispatch_task_queue, elt, count);
  return count;
}
