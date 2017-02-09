#include <limits.h>

#include "object_info.h"
#include "task.h"
#include "state/task_table.h"

#include "global_scheduler_algorithm.h"

global_scheduler_policy_state *init_global_scheduler_policy(void) {
  global_scheduler_policy_state *policy_state =
      malloc(sizeof(global_scheduler_policy_state));
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

void destroy_global_scheduler_policy(
    global_scheduler_policy_state *policy_state) {
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
bool constraints_satisfied_hard(const local_scheduler *scheduler,
                                const task_spec *spec) {
  for (int i = 0; i < MAX_RESOURCE_INDEX; i++) {
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
void handle_task_round_robin(global_scheduler_state *state,
                             global_scheduler_policy_state *policy_state,
                             task *task) {
  CHECKM(utarray_len(state->local_schedulers) > 0,
         "No local schedulers. We currently don't handle this case.");
  local_scheduler *scheduler = NULL;
  task_spec *task_spec = task_task_spec(task);
  int i;
  int num_retries = 1;
  bool task_satisfied = false;

  for (i = policy_state->round_robin_index; !task_satisfied && num_retries > 0;
       i = (i + 1) % utarray_len(state->local_schedulers)) {
    if (i == policy_state->round_robin_index) {
      num_retries--;
    }
    scheduler = (local_scheduler *) utarray_eltptr(state->local_schedulers, i);
    task_satisfied = constraints_satisfied_hard(scheduler, task_spec);
  }

  if (task_satisfied) {
    /* Update next index to try and assign the task. */
    policy_state->round_robin_index = i; /* i was advanced. */
    assign_task_to_local_scheduler(state, task, scheduler->id);
  } else {
    /* TODO(atumanov): propagate the error to the driver, which submitted
     * this impossible task and/or cache the task to consider when new
     * local schedulers register.
     */
  }
}

object_size_entry *create_object_size_hashmap(global_scheduler_state *state,
                                              task_spec *task_spec,
                                              bool *has_args_by_ref,
                                              int64_t *task_data_size) {
  object_size_entry *s = NULL, *object_size_table = NULL;
  *task_data_size = 0;

  for (int i = 0; i < task_num_args(task_spec); i++) {
    /* Object ids are only available for args by references.
     * Args by value are serialized into the task_spec itself.
     * We will only concern ourselves with args by ref for data size calculation
     */
    if (task_arg_type(task_spec, i) != ARG_BY_REF) {
      continue;
    }
    *has_args_by_ref = true;
    object_id obj_id = task_arg_id(task_spec, i);
    /* Look up this object ID in the global scheduler object cache. */
    scheduler_object_info *obj_info_entry = NULL;
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
    *task_data_size += object_size; /* Add each object's size to task's size. */
    char **p = NULL;
    char id_string[ID_STRING_SIZE];
    LOG_DEBUG("locations for an arg_by_ref obj_id = %s",
              object_id_to_string(obj_id, id_string, ID_STRING_SIZE));
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
        s = calloc(1, sizeof(object_size_entry));
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

void free_object_size_hashmap(object_size_entry *object_size_table) {
  /* Destroy local state. */
  object_size_entry *tmp, *s = NULL;
  HASH_ITER(hh, object_size_table, s, tmp) {
    HASH_DEL(object_size_table, s);
    /* NOTE: Do not free externally stored s->object_location. */
    free(s);
  }
}

db_client_id get_photon_id(global_scheduler_state *state,
                           const char *plasma_location) {
  aux_address_entry *aux_entry = NULL;
  db_client_id photon_id = NIL_ID;
  if (plasma_location != NULL) {
    LOG_DEBUG("max object size location found : %s", plasma_location);
    /* Lookup association of plasma location to photon. */
    HASH_FIND(plasma_photon_hh, state->plasma_photon_map, plasma_location,
              uthash_strlen(plasma_location), aux_entry);
    if (aux_entry) {
      LOG_DEBUG("found photon db client association for plasma ip:port = %s",
                aux_entry->aux_address);
      /* Plasma to photon db client ID association found, get photon ID. */
      photon_id = aux_entry->photon_db_client_id;
    } else {
      LOG_ERROR("photon db client association not found for plasma ip:port=%s",
                plasma_location);
    }
  }

  char id_string[ID_STRING_SIZE];
  LOG_DEBUG("photon ID found = %s",
            object_id_to_string(photon_id, id_string, ID_STRING_SIZE));
  UNUSED(id_string);

  if (IS_NIL_ID(photon_id)) {
    return photon_id;
  }

  /* Check to make sure this photon_db_client_id matches one of the
   * schedulers. */
  local_scheduler *local_scheduler_ptr = get_local_scheduler(state, photon_id);
  if (local_scheduler_ptr == NULL) {
    LOG_WARN("photon_id didn't match any cached local scheduler entries");
  }
  return photon_id;
}

double inner_product(double a[], double b[], int size) {
  double result = 0;
  for (int i = 0; i < size; i++) {
    result += a[i] * b[i];
  }
  return result;
}

double calculate_object_size_fraction(global_scheduler_state *state,
                                      local_scheduler *scheduler,
                                      object_size_entry *object_size_table,
                                      int64_t total_task_object_size) {
  /* Look up its cached object size in the hashmap, normalize by total object
   * size for this task. */
  /* Aggregate object size for this task. */
  double object_size_fraction = 0;
  if (total_task_object_size > 0) {
    /* Does this node contribute anything to this task object size? */
    /* Lookup scheduler->id in photon_plasma_map to get plasma aux address,
     * which is used as the key for object_size_table.
     * The use the plasma aux address to locate object_size this node
     * contributes.
     */
    aux_address_entry *photon_plasma_pair = NULL;
    HASH_FIND(photon_plasma_hh, state->photon_plasma_map, &(scheduler->id),
              sizeof(scheduler->id), photon_plasma_pair);
    if (photon_plasma_pair != NULL) {
      object_size_entry *s = NULL;
      /* Found this node's photon to plasma mapping. Use the corresponding
       * plasma key to see if this node has any cached objects for this task. */
      HASH_FIND_STR(object_size_table, photon_plasma_pair->aux_address, s);
      if (s != NULL) {
        /* This node has some of this task's objects. Calculate what fraction.
         */
        CHECK(strcmp(s->object_location, photon_plasma_pair->aux_address) == 0);
        object_size_fraction =
            MIN(1, (double) (s->total_object_size) / total_task_object_size);
      }
    }
  }
  return object_size_fraction;
}

double calculate_score_dynvec_normalized(global_scheduler_state *state,
                                         local_scheduler *scheduler,
                                         const task_spec *task_spec,
                                         double object_size_fraction) {
  /* The object size fraction is now calculated for this (task,node) pair. */
  /* Construct the normalized dynamic resource attribute vector */
  double normalized_dynvec[MAX_RESOURCE_INDEX + 1];
  memset(&normalized_dynvec, 0, sizeof(normalized_dynvec));
  for (int i = 0; i < MAX_RESOURCE_INDEX; i++) {
    double resreqval = task_spec_get_required_resource(task_spec, i);
    if (resreqval <= 0) {
      /* Skip and leave normalized dynvec value == 0. */
      continue;
    }
    normalized_dynvec[i] =
        MIN(1, scheduler->info.dynamic_resources[i] / resreqval);
  }
  normalized_dynvec[MAX_RESOURCE_INDEX] = object_size_fraction;

  /* Finally, calculate the score. */
  double score = inner_product(normalized_dynvec,
                               state->policy_state->resource_attribute_weight,
                               MAX_RESOURCE_INDEX + 1);
  return score;
}

double calculate_cost_pending(const global_scheduler_state *state,
                              const local_scheduler *scheduler) {
  /* TODO: make sure that num_recent_tasks_sent is reset on each heartbeat. */
  return scheduler->num_recent_tasks_sent + scheduler->info.task_queue_length;
}

/**
 * Main new task handling function in the global scheduler.
 *
 * @param state Global scheduler state.
 * @param policy_state State specific to the scheduling policy.
 * @param task New task to be scheduled.
 * @return Void.
 */
void handle_task_waiting(global_scheduler_state *state,
                         global_scheduler_policy_state *policy_state,
                         task *task) {
  task_spec *task_spec = task_task_spec(task);

  CHECKM(task_spec != NULL,
         "task wait handler encounted a task with NULL spec");
  /* Local hash table to keep track of aggregate object sizes per local
   * scheduler. */
  object_size_entry *object_size_table = NULL;
  bool has_args_by_ref = false;
  bool task_feasible = false;
  /* The total size of the task's data. */
  int64_t task_object_size = 0;

  object_size_table = create_object_size_hashmap(
      state, task_spec, &has_args_by_ref, &task_object_size);

  /* Go through all the nodes, calculate the score for each, pick max score. */
  local_scheduler *scheduler = NULL;
  double best_photon_score = INT32_MIN;
  CHECKM(best_photon_score < 0, "We might have a floating point underflow");
  db_client_id best_photon_id = NIL_ID; /* best node to send this task */
  for (scheduler = (local_scheduler *) utarray_front(state->local_schedulers);
       scheduler != NULL;
       scheduler = (local_scheduler *) utarray_next(
           state->local_schedulers, scheduler)) {
    /* For each local scheduler, calculate its score. Check hard constraints
     * first. */
    if (!constraints_satisfied_hard(scheduler, task_spec)) {
      continue;
    }
    task_feasible = true;
    /* This node satisfies the hard capacity constraint. Calculate its score. */
    double score = -1 * calculate_cost_pending(state, scheduler);
    if (score > best_photon_score) {
      best_photon_score = score;
      best_photon_id = scheduler->id;
    }
  } /* For each local scheduler. */

  free_object_size_hashmap(object_size_table);

  if (!task_feasible) {
    char id_string[ID_STRING_SIZE];
    LOG_ERROR(
        "Infeasible task. No nodes satisfy hard constraints for task = %s",
        object_id_to_string(task_task_id(task), id_string, ID_STRING_SIZE));
    /* TODO(atumanov): propagate this error to the task's driver and/or
     * cache the task in case new local schedulers satisfy it in the future. */
    return;
  }
  CHECKM(!IS_NIL_ID(best_photon_id),
         "Task is feasible, but doesn't have a local scheduler assigned.");
  /* A local scheduler ID was found, so assign the task. */
  assign_task_to_local_scheduler(state, task, best_photon_id);
}

void handle_object_available(global_scheduler_state *state,
                             global_scheduler_policy_state *policy_state,
                             object_id object_id) {
  /* Do nothing for now. */
}

void handle_new_local_scheduler(global_scheduler_state *state,
                                global_scheduler_policy_state *policy_state,
                                db_client_id db_client_id) {
  /* Do nothing for now. */
}
