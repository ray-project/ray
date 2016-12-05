#include "photon_algorithm.h"

#include <stdbool.h>
#include "utarray.h"
#include "utlist.h"

#include "state/task_table.h"
#include "photon.h"
#include "photon_scheduler.h"

/* TODO(swang): We should set retry values in a config file somewhere. */
const retry_info photon_retry = {0, 1000, NULL};

typedef struct task_queue_entry {
  /** The task that is queued. */
  task_spec *spec;
  /** True if this task was assigned to this local scheduler by the global
   *  scheduler and false otherwise. */
  bool from_global_scheduler;
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
  /** An array of pointers to tasks that are waiting to be scheduled. */
  task_queue_entry *task_queue;
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
  algorithm_state->task_queue = NULL;
  utarray_new(algorithm_state->available_workers, &ut_int_icd);
  /* Initialize the hash table of objects being fetched. */
  algorithm_state->fetch_requests = NULL;
  return algorithm_state;
}

void free_scheduling_algorithm_state(
    scheduling_algorithm_state *algorithm_state) {
  task_queue_entry *elt, *tmp1;
  DL_FOREACH_SAFE(algorithm_state->task_queue, elt, tmp1) {
    DL_DELETE(algorithm_state->task_queue, elt);
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
  plasma_fetch2(fetch_req->state->plasma_conn, 1, object_ids);
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
        plasma_fetch2(state->plasma_conn, 1, object_ids);
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
 * If there is a task whose dependencies are available locally, assign it to the
 * worker. This does not remove the worker from the available worker queue.
 *
 * @param s The scheduler state.
 * @param worker_index The index of the worker.
 * @return This returns 1 if it successfully assigned a task to the worker,
 *         otherwise it returns 0.
 */
bool find_and_schedule_task_if_possible(
    local_scheduler_state *state,
    scheduling_algorithm_state *algorithm_state,
    int worker_index) {
  task_queue_entry *elt, *tmp;
  bool found_task_to_schedule = false;
  /* Find the first task whose dependencies are available locally. */
  DL_FOREACH_SAFE(algorithm_state->task_queue, elt, tmp) {
    if (can_run(algorithm_state, elt->spec)) {
      found_task_to_schedule = true;
      break;
    }
  }
  if (found_task_to_schedule) {
    /* This task's dependencies are available locally, so assign the task to the
     * worker. */
    assign_task_to_worker(state, elt->spec, worker_index,
                          elt->from_global_scheduler);
    /* Update the task queue data structure and free the task. */
    DL_DELETE(algorithm_state->task_queue, elt);
    free_task_spec(elt->spec);
    free(elt);
  }
  return found_task_to_schedule;
}

void run_task_immediately(local_scheduler_state *state,
                          scheduling_algorithm_state *algorithm_state,
                          task_spec *spec,
                          bool from_global_scheduler) {
  /* Get the last available worker in the available worker queue. */
  int *worker_index = (int *) utarray_back(algorithm_state->available_workers);
  /* Tell the available worker to execute the task. */
  assign_task_to_worker(state, spec, *worker_index, from_global_scheduler);
  /* Remove the available worker from the queue and free the struct. */
  utarray_pop_back(algorithm_state->available_workers);
}

void queue_task_locally(local_scheduler_state *state,
                        scheduling_algorithm_state *algorithm_state,
                        task_spec *spec,
                        bool from_global_scheduler) {
  /* Copy the spec and add it to the task queue. The allocated spec will be
   * freed when it is assigned to a worker. */
  task_queue_entry *elt = malloc(sizeof(task_queue_entry));
  elt->spec = (task_spec *) malloc(task_spec_size(spec));
  memcpy(elt->spec, spec, task_spec_size(spec));
  elt->from_global_scheduler = from_global_scheduler;
  DL_APPEND(algorithm_state->task_queue, elt);
}

void give_task_to_global_scheduler(local_scheduler_state *state,
                                   scheduling_algorithm_state *algorithm_state,
                                   task_spec *spec,
                                   bool from_global_scheduler) {
  /* Pass on the task to the global scheduler. */
  DCHECK(!from_global_scheduler);
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
  if ((utarray_len(algorithm_state->available_workers) > 0) &&
      can_run(algorithm_state, spec)) {
    run_task_immediately(state, algorithm_state, spec, false);
  } else if (state->db == NULL) {
    queue_task_locally(state, algorithm_state, spec, false);
  } else {
    give_task_to_global_scheduler(state, algorithm_state, spec, false);
  }
}

void handle_task_scheduled(local_scheduler_state *state,
                           scheduling_algorithm_state *algorithm_state,
                           task_spec *spec) {
  /* This callback handles tasks that were assigned to this local scheduler by
   * the global scheduler, so we can safely assert that there is a connection
   * to the database. */
  DCHECK(state->db != NULL);
  /* Initiate fetch calls for any dependencies that are not present locally. */
  fetch_missing_dependencies(state, algorithm_state, spec);
  /* If this task's dependencies are available locally, and if there is an
   * available worker, then assign this task to an available worker. If we
   * cannot assign the task to a worker immediately, queue the task locally. */
  if ((utarray_len(algorithm_state->available_workers) > 0) &&
      can_run(algorithm_state, spec)) {
    run_task_immediately(state, algorithm_state, spec, true);
  } else {
    queue_task_locally(state, algorithm_state, spec, true);
  }
}

void handle_worker_available(local_scheduler_state *state,
                             scheduling_algorithm_state *algorithm_state,
                             int worker_index) {
  int scheduled_task =
      find_and_schedule_task_if_possible(state, algorithm_state, worker_index);
  /* If we couldn't find a task to schedule, add the worker to the queue of
   * available workers. */
  if (!scheduled_task) {
    for (int *p = (int *) utarray_front(algorithm_state->available_workers);
         p != NULL;
         p = (int *) utarray_next(algorithm_state->available_workers, p)) {
      DCHECK(*p != worker_index);
    }
    /* Add client_sock to a list of available workers. This struct will be freed
     * when a task is assigned to this worker. */
    utarray_push_back(algorithm_state->available_workers, &worker_index);
    LOG_DEBUG("Adding worker_index %d to available workers.\n", worker_index);
  }
}

void handle_object_available(local_scheduler_state *state,
                             scheduling_algorithm_state *algorithm_state,
                             object_id object_id) {
  /* TODO(rkn): When does this get freed? */
  available_object *entry =
      (available_object *) malloc(sizeof(available_object));
  entry->object_id = object_id;
  HASH_ADD(handle, algorithm_state->local_objects, object_id, sizeof(object_id),
           entry);

  /* Check if we can schedule any tasks. */
  int num_tasks_scheduled = 0;
  for (int *p = (int *) utarray_front(algorithm_state->available_workers);
       p != NULL;
       p = (int *) utarray_next(algorithm_state->available_workers, p)) {
    /* Schedule a task on this worker if possible. */
    int scheduled_task =
        find_and_schedule_task_if_possible(state, algorithm_state, *p);
    if (!scheduled_task) {
      /* There are no tasks we can schedule, so exit the loop. */
      break;
    }
    num_tasks_scheduled += 1;
  }
  utarray_erase(algorithm_state->available_workers, 0, num_tasks_scheduled);

  /* If we were previously trying to fetch this object, remove the fetch request
   * from the hash table. */
  fetch_object_request *fetch_req;
  HASH_FIND(hh, algorithm_state->fetch_requests, &object_id, sizeof(object_id),
            fetch_req);
  if (fetch_req != NULL) {
    HASH_DELETE(hh, algorithm_state->fetch_requests, fetch_req);
    event_loop_remove_timer(state->loop, fetch_req->timer);
    free(fetch_req);
  }
}
