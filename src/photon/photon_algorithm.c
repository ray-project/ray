#include "photon_algorithm.h"

#include <stdbool.h>
#include "utarray.h"
#include "utlist.h"

#include "state/task_table.h"
#include "photon.h"
#include "photon_scheduler.h"

/* TODO(swang): We should set retry values in a config file somewhere. */
const retry_info photon_retry = {
    .num_retries = 0,
    .timeout = 1000,
    .fail_callback = NULL,
};

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

/** Part of the photon state that is maintained by the scheduling algorithm. */
struct scheduler_state {
  /** An array of pointers to tasks that are waiting to be scheduled. */
  task_queue_entry *task_queue;
  /** An array of worker indices corresponding to clients that are
   *  waiting for tasks. */
  UT_array *available_workers;
  /** A hash map of the objects that are available in the local Plasma store.
   *  This information could be a little stale. */
  available_object *local_objects;
};

scheduler_state *make_scheduler_state(void) {
  scheduler_state *state = malloc(sizeof(scheduler_state));
  /* Initialize an empty hash map for the cache of local available objects. */
  state->local_objects = NULL;
  /* Initialize the local data structures used for queuing tasks and workers. */
  state->task_queue = NULL;
  utarray_new(state->available_workers, &ut_int_icd);
  return state;
}

void free_scheduler_state(scheduler_state *s) {
  task_queue_entry *elt, *tmp1;
  DL_FOREACH_SAFE(s->task_queue, elt, tmp1) {
    DL_DELETE(s->task_queue, elt);
    free_task_spec(elt->spec);
    free(elt);
  }
  utarray_free(s->available_workers);
  available_object *available_obj, *tmp2;
  HASH_ITER(handle, s->local_objects, available_obj, tmp2) {
    HASH_DELETE(handle, s->local_objects, available_obj);
    free(available_obj);
  }
  free(s);
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
bool can_run(scheduler_state *s, task_spec *task) {
  int64_t num_args = task_num_args(task);
  for (int i = 0; i < num_args; ++i) {
    if (task_arg_type(task, i) == ARG_BY_REF) {
      object_id obj_id = task_arg_id(task, i);
      available_object *entry;
      HASH_FIND(handle, s->local_objects, &obj_id, sizeof(object_id), entry);
      if (entry == NULL) {
        /* The object is not present locally, so this task cannot be scheduled
         * right now. */
        return false;
      }
    }
  }
  return true;
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
bool find_and_schedule_task_if_possible(scheduler_info *info,
                                        scheduler_state *state,
                                        int worker_index) {
  task_queue_entry *elt, *tmp;
  bool found_task_to_schedule = false;
  /* Find the first task whose dependencies are available locally. */
  DL_FOREACH_SAFE(state->task_queue, elt, tmp) {
    if (can_run(state, elt->spec)) {
      found_task_to_schedule = true;
      break;
    }
  }
  if (found_task_to_schedule) {
    /* This task's dependencies are available locally, so assign the task to the
     * worker. */
    assign_task_to_worker(info, elt->spec, worker_index,
                          elt->from_global_scheduler);
    /* Update the task queue data structure and free the task. */
    DL_DELETE(state->task_queue, elt);
    free_task_spec(elt->spec);
    free(elt);
  }
  return found_task_to_schedule;
}

void run_task_immediately(scheduler_info *info,
                          scheduler_state *s,
                          task_spec *spec,
                          bool from_global_scheduler) {
  /* Get the last available worker in the available worker queue. */
  int *worker_index = (int *) utarray_back(s->available_workers);
  /* Tell the available worker to execute the task. */
  assign_task_to_worker(info, spec, *worker_index, from_global_scheduler);
  /* Remove the available worker from the queue and free the struct. */
  utarray_pop_back(s->available_workers);
}

void queue_task_locally(scheduler_info *info,
                        scheduler_state *s,
                        task_spec *spec,
                        bool from_global_scheduler) {
  /* Copy the spec and add it to the task queue. The allocated spec will be
   * freed when it is assigned to a worker. */
  task_queue_entry *elt = malloc(sizeof(task_queue_entry));
  elt->spec = malloc(task_spec_size(spec));
  memcpy(elt->spec, spec, task_spec_size(spec));
  elt->from_global_scheduler = from_global_scheduler;
  DL_APPEND(s->task_queue, elt);
}

void give_task_to_global_scheduler(scheduler_info *info,
                                   scheduler_state *s,
                                   task_spec *spec,
                                   bool from_global_scheduler) {
  /* Pass on the task to the global scheduler. */
  DCHECK(!from_global_scheduler);
  task *task = alloc_task(spec, TASK_STATUS_WAITING, NIL_ID);
  DCHECK(info->db != NULL);
  task_table_add_task(info->db, task, (retry_info *) &photon_retry, NULL, NULL);
  free_task(task);
}

void handle_task_submitted(scheduler_info *info,
                           scheduler_state *s,
                           task_spec *spec) {
  /* If this task's dependencies are available locally, and if there is an
   * available worker, then assign this task to an available worker. If we
   * cannot assign the task to a worker immediately, we either queue the task in
   * the local task queue or we pass the task to the global scheduler. For now,
   * we pass the task along to the global scheduler if there is one. */
  if ((utarray_len(s->available_workers) > 0) && can_run(s, spec)) {
    run_task_immediately(info, s, spec, false);
  } else if (info->db == NULL) {
    queue_task_locally(info, s, spec, false);
  } else {
    give_task_to_global_scheduler(info, s, spec, false);
  }
}

void handle_task_scheduled(scheduler_info *info,
                           scheduler_state *s,
                           task_spec *spec) {
  /* This callback handles tasks that were assigned to this local scheduler by
   * the global scheduler, so we can safely assert that there is a connection
   * to the database. */
  DCHECK(info->db != NULL);
  /* If this task's dependencies are available locally, and if there is an
   * available worker, then assign this task to an available worker. If we
   * cannot assign the task to a worker immediately, queue the task locally. */
  if ((utarray_len(s->available_workers) > 0) && can_run(s, spec)) {
    run_task_immediately(info, s, spec, true);
  } else {
    queue_task_locally(info, s, spec, true);
  }
}

void handle_worker_available(scheduler_info *info,
                             scheduler_state *state,
                             int worker_index) {
  int scheduled_task =
      find_and_schedule_task_if_possible(info, state, worker_index);
  /* If we couldn't find a task to schedule, add the worker to the queue of
   * available workers. */
  if (!scheduled_task) {
    for (int *p = (int *) utarray_front(state->available_workers); p != NULL;
         p = (int *) utarray_next(state->available_workers, p)) {
      DCHECK(*p != worker_index);
    }
    /* Add client_sock to a list of available workers. This struct will be freed
     * when a task is assigned to this worker. */
    utarray_push_back(state->available_workers, &worker_index);
    LOG_DEBUG("Adding worker_index %d to available workers.\n", worker_index);
  }
}

void handle_object_available(scheduler_info *info,
                             scheduler_state *state,
                             object_id object_id) {
  /* TODO(rkn): When does this get freed? */
  available_object *entry =
      (available_object *) malloc(sizeof(available_object));
  entry->object_id = object_id;
  HASH_ADD(handle, state->local_objects, object_id, sizeof(object_id), entry);

  /* Check if we can schedule any tasks. */
  int num_tasks_scheduled = 0;
  for (int *p = (int *) utarray_front(state->available_workers); p != NULL;
       p = (int *) utarray_next(state->available_workers, p)) {
    /* Schedule a task on this worker if possible. */
    int scheduled_task = find_and_schedule_task_if_possible(info, state, *p);
    if (!scheduled_task) {
      /* There are no tasks we can schedule, so exit the loop. */
      break;
    }
    num_tasks_scheduled += 1;
  }
  utarray_erase(state->available_workers, 0, num_tasks_scheduled);
}
