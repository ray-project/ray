#include <limits.h>

#include "object_info.h"
#include "task.h"
#include "state/task_table.h"

#include "global_scheduler_algorithm.h"

GlobalSchedulerPolicyState *GlobalSchedulerPolicyState_init(void) {
  GlobalSchedulerPolicyState *policy_state =
      (GlobalSchedulerPolicyState *) malloc(sizeof(GlobalSchedulerPolicyState));
  policy_state->round_robin_index = 0;

  int num_weight_elem =
      sizeof(policy_state->resource_attribute_weight) / sizeof(double);
  for (int i = 0; i < num_weight_elem; i++) {
    /* Weight distribution is subject to scheduling policy. Giving all weight
     * to the last element of the vector (cached data) is equivalent to
     * the transfer-aware policy. */
    policy_state->resource_attribute_weight[i] = 1.0 / num_weight_elem;
  }

  return policy_state;
}

void GlobalSchedulerPolicyState_free(GlobalSchedulerPolicyState *policy_state) {
  free(policy_state);
}

/**
 * Checks if the given local scheduler satisfies the task's hard constraints.
 *
 * @param scheduler Local scheduler.
 * @param spec Task specification.
 * @return True if all tasks's resource constraints are satisfied. False
 *         otherwise.
 */
bool constraints_satisfied_hard(const LocalScheduler *scheduler,
                                const task_spec *spec) {
  for (int i = 0; i < ResourceIndex_MAX; i++) {
    if (scheduler->info.static_resources[i] <
        task_spec_get_required_resource(spec, i)) {
      return false;
    }
  }
  return true;
}

/**
 * This is a helper method that assigns a task to the next local scheduler in a
 * round robin fashion.
 */
void handle_task_round_robin(GlobalSchedulerState *state,
                             GlobalSchedulerPolicyState *policy_state,
                             Task *task) {
  CHECKM(utarray_len(state->local_schedulers) > 0,
         "No local schedulers. We currently don't handle this case.");
  LocalScheduler *scheduler = NULL;
  task_spec *task_spec = Task_task_spec(task);
  int i;
  int num_retries = 1;
  bool task_satisfied = false;

  for (i = policy_state->round_robin_index; !task_satisfied && num_retries > 0;
       i = (i + 1) % utarray_len(state->local_schedulers)) {
    if (i == policy_state->round_robin_index) {
      num_retries--;
    }
    scheduler = (LocalScheduler *) utarray_eltptr(state->local_schedulers, i);
    task_satisfied = constraints_satisfied_hard(scheduler, task_spec);
  }

  if (task_satisfied) {
    /* Update next index to try and assign the task. Note that the counter i has
     * been advanced. */
    policy_state->round_robin_index = i;
    assign_task_to_local_scheduler(state, task, scheduler->id);
  } else {
    /* TODO(atumanov): propagate the error to the driver, which submitted
     * this impossible task and/or cache the task to consider when new
     * local schedulers register. */
  }
}

ObjectSizeEntry *create_object_size_hashmap(GlobalSchedulerState *state,
                                            task_spec *task_spec,
                                            bool *has_args_by_ref,
                                            int64_t *task_data_size) {
  ObjectSizeEntry *s = NULL, *object_size_table = NULL;
  *task_data_size = 0;

  for (int i = 0; i < task_num_args(task_spec); i++) {
    /* Object ids are only available for args by references.
     * Args by value are serialized into the task_spec itself.
     * We will only concern ourselves with args by ref for data size calculation
     */
    if (!task_arg_by_ref(task_spec, i)) {
      continue;
    }
    *has_args_by_ref = true;
    ObjectID obj_id = task_arg_id(task_spec, i);
    /* Look up this object ID in the global scheduler object cache. */
    SchedulerObjectInfo *obj_info_entry = NULL;
    HASH_FIND(hh, state->scheduler_object_info_table, &obj_id, sizeof(obj_id),
              obj_info_entry);
    if (obj_info_entry == NULL) {
      /* Global scheduler doesn't know anything about this object ID, so skip
       * it. */
      LOG_DEBUG("Processing task with object ID not known to global scheduler");
      continue;
    }
    LOG_DEBUG("[GS] found object id, data_size = %" PRId64,
              obj_info_entry->data_size);
    /* Object is known to the scheduler. For each of its locations, add size. */
    int64_t object_size = obj_info_entry->data_size;
    /* Add each object's size to task's size. */
    *task_data_size += object_size;
    char **p = NULL;
    char id_string[ID_STRING_SIZE];
    LOG_DEBUG("locations for an arg_by_ref obj_id = %s",
              ObjectID_to_string(obj_id, id_string, ID_STRING_SIZE));
    UNUSED(id_string);
    for (p = (char **) utarray_front(obj_info_entry->object_locations);
         p != NULL;
         p = (char **) utarray_next(obj_info_entry->object_locations, p)) {
      const char *object_location = *p;

      LOG_DEBUG("\tobject location: %s", object_location);

      /* Look up this location in the local object size hash table. */
      HASH_FIND_STR(object_size_table, object_location, s);
      if (NULL == s) {
        /* This location not yet known, so add this object location. */
        s = (ObjectSizeEntry *) calloc(1, sizeof(ObjectSizeEntry));
        s->object_location = object_location;
        HASH_ADD_KEYPTR(hh, object_size_table, s->object_location,
                        strlen(s->object_location), s);
      }
      /* At this point the object location exists in our hash table. */
      s->total_object_size += object_size;
    } /* End for each object's location. */
  }   /* End for each task's object. */

  return object_size_table;
}

void free_object_size_hashmap(ObjectSizeEntry *object_size_table) {
  /* Destroy local state. */
  ObjectSizeEntry *tmp, *s = NULL;
  HASH_ITER(hh, object_size_table, s, tmp) {
    HASH_DEL(object_size_table, s);
    /* NOTE: Do not free externally stored s->object_location. */
    free(s);
  }
}

DBClientID get_local_scheduler_id(GlobalSchedulerState *state,
                                  const char *plasma_location) {
  AuxAddressEntry *aux_entry = NULL;
  DBClientID local_scheduler_id = NIL_ID;
  if (plasma_location != NULL) {
    LOG_DEBUG("max object size location found : %s", plasma_location);
    /* Lookup association of plasma location to local scheduler. */
    HASH_FIND(plasma_local_scheduler_hh, state->plasma_local_scheduler_map,
              plasma_location, uthash_strlen(plasma_location), aux_entry);
    if (aux_entry) {
      LOG_DEBUG(
          "found local scheduler db client association for plasma ip:port = %s",
          aux_entry->aux_address);
      /* Plasma to local scheduler db client ID association found, get local
       * scheduler ID. */
      local_scheduler_id = aux_entry->local_scheduler_db_client_id;
    } else {
      LOG_ERROR(
          "local scheduler db client association not found for plasma "
          "ip:port=%s",
          plasma_location);
    }
  }

  char id_string[ID_STRING_SIZE];
  LOG_DEBUG("local scheduler ID found = %s",
            ObjectID_to_string(local_scheduler_id, id_string, ID_STRING_SIZE));
  UNUSED(id_string);

  if (IS_NIL_ID(local_scheduler_id)) {
    return local_scheduler_id;
  }

  /* Check to make sure this local_scheduler_db_client_id matches one of the
   * schedulers. */
  LocalScheduler *local_scheduler_ptr =
      get_local_scheduler(state, local_scheduler_id);
  if (local_scheduler_ptr == NULL) {
    LOG_WARN(
        "local_scheduler_id didn't match any cached local scheduler entries");
  }
  return local_scheduler_id;
}

double inner_product(double a[], double b[], int size) {
  double result = 0;
  for (int i = 0; i < size; i++) {
    result += a[i] * b[i];
  }
  return result;
}

double calculate_object_size_fraction(GlobalSchedulerState *state,
                                      LocalScheduler *scheduler,
                                      ObjectSizeEntry *object_size_table,
                                      int64_t total_task_object_size) {
  /* Look up its cached object size in the hashmap, normalize by total object
   * size for this task. */
  /* Aggregate object size for this task. */
  double object_size_fraction = 0;
  if (total_task_object_size > 0) {
    /* Does this node contribute anything to this task object size? */
    /* Lookup scheduler->id in local_scheduler_plasma_map to get plasma aux
     * address, which is used as the key for object_size_table. This uses the
     * plasma aux address to locate the object_size this node contributes. */
    AuxAddressEntry *local_scheduler_plasma_pair = NULL;
    HASH_FIND(local_scheduler_plasma_hh, state->local_scheduler_plasma_map,
              &(scheduler->id), sizeof(scheduler->id),
              local_scheduler_plasma_pair);
    if (local_scheduler_plasma_pair != NULL) {
      ObjectSizeEntry *s = NULL;
      /* Found this node's local scheduler to plasma mapping. Use the
       * corresponding plasma key to see if this node has any cached objects for
       * this task. */
      HASH_FIND_STR(object_size_table, local_scheduler_plasma_pair->aux_address,
                    s);
      if (s != NULL) {
        /* This node has some of this task's objects. Calculate what fraction.
         */
        CHECK(strcmp(s->object_location,
                     local_scheduler_plasma_pair->aux_address) == 0);
        object_size_fraction =
            MIN(1, (double) (s->total_object_size) / total_task_object_size);
      }
    }
  }
  return object_size_fraction;
}

double calculate_score_dynvec_normalized(GlobalSchedulerState *state,
                                         LocalScheduler *scheduler,
                                         const task_spec *task_spec,
                                         double object_size_fraction) {
  /* The object size fraction is now calculated for this (task,node) pair. */
  /* Construct the normalized dynamic resource attribute vector */
  double normalized_dynvec[ResourceIndex_MAX + 1];
  memset(&normalized_dynvec, 0, sizeof(normalized_dynvec));
  for (int i = 0; i < ResourceIndex_MAX; i++) {
    double resreqval = task_spec_get_required_resource(task_spec, i);
    if (resreqval <= 0) {
      /* Skip and leave normalized dynvec value == 0. */
      continue;
    }
    normalized_dynvec[i] =
        MIN(1, scheduler->info.dynamic_resources[i] / resreqval);
  }
  normalized_dynvec[ResourceIndex_MAX] = object_size_fraction;

  /* Finally, calculate the score. */
  double score = inner_product(normalized_dynvec,
                               state->policy_state->resource_attribute_weight,
                               ResourceIndex_MAX + 1);
  return score;
}

double calculate_cost_pending(const GlobalSchedulerState *state,
                              const LocalScheduler *scheduler) {
  /* TODO: make sure that num_recent_tasks_sent is reset on each heartbeat. */
  return scheduler->num_recent_tasks_sent + scheduler->info.task_queue_length;
}

bool handle_task_waiting(GlobalSchedulerState *state,
                         GlobalSchedulerPolicyState *policy_state,
                         Task *task) {
  task_spec *task_spec = Task_task_spec(task);

  CHECKM(task_spec != NULL,
         "task wait handler encounted a task with NULL spec");
  /* Local hash table to keep track of aggregate object sizes per local
   * scheduler. */
  ObjectSizeEntry *object_size_table = NULL;
  bool has_args_by_ref = false;
  bool task_feasible = false;
  /* The total size of the task's data. */
  int64_t task_object_size = 0;

  object_size_table = create_object_size_hashmap(
      state, task_spec, &has_args_by_ref, &task_object_size);

  /* Go through all the nodes, calculate the score for each, pick max score. */
  LocalScheduler *scheduler = NULL;
  double best_local_scheduler_score = INT32_MIN;
  CHECKM(best_local_scheduler_score < 0,
         "We might have a floating point underflow");
  DBClientID best_local_scheduler_id = NIL_ID; /* best node to send this task */
  for (scheduler = (LocalScheduler *) utarray_front(state->local_schedulers);
       scheduler != NULL; scheduler = (LocalScheduler *) utarray_next(
                              state->local_schedulers, scheduler)) {
    /* For each local scheduler, calculate its score. Check hard constraints
     * first. */
    if (!constraints_satisfied_hard(scheduler, task_spec)) {
      continue;
    }
    task_feasible = true;
    /* This node satisfies the hard capacity constraint. Calculate its score. */
    double score = -1 * calculate_cost_pending(state, scheduler);
    if (score > best_local_scheduler_score) {
      best_local_scheduler_score = score;
      best_local_scheduler_id = scheduler->id;
    }
  } /* For each local scheduler. */

  free_object_size_hashmap(object_size_table);

  if (!task_feasible) {
    char id_string[ID_STRING_SIZE];
    LOG_ERROR(
        "Infeasible task. No nodes satisfy hard constraints for task = %s",
        ObjectID_to_string(Task_task_id(task), id_string, ID_STRING_SIZE));
    /* TODO(atumanov): propagate this error to the task's driver and/or
     * cache the task in case new local schedulers satisfy it in the future. */
    return false;
  }
  CHECKM(!IS_NIL_ID(best_local_scheduler_id),
         "Task is feasible, but doesn't have a local scheduler assigned.");
  /* A local scheduler ID was found, so assign the task. */
  assign_task_to_local_scheduler(state, task, best_local_scheduler_id);
  return true;
}

void handle_object_available(GlobalSchedulerState *state,
                             GlobalSchedulerPolicyState *policy_state,
                             ObjectID object_id) {
  /* Do nothing for now. */
}

void handle_new_local_scheduler(GlobalSchedulerState *state,
                                GlobalSchedulerPolicyState *policy_state,
                                DBClientID db_client_id) {
  /* Do nothing for now. */
}
