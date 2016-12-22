#include "object_info.h"
#include "task.h"
#include "task_table.h"

#include "global_scheduler_algorithm.h"

global_scheduler_policy_state *init_global_scheduler_policy(void) {
  global_scheduler_policy_state *policy_state =
      malloc(sizeof(global_scheduler_policy_state));
  policy_state->round_robin_index = 0;
  return policy_state;
}

void destroy_global_scheduler_policy(
    global_scheduler_policy_state *policy_state) {
  free(policy_state);
}

void handle_task_roundrobin(global_scheduler_state *state,
                            global_scheduler_policy_state *policy_state,
                            task *task) {
  if (utarray_len(state->local_schedulers) > 0) {
    local_scheduler *scheduler = (local_scheduler *) utarray_eltptr(
        state->local_schedulers, policy_state->round_robin_index);
    scheduler->num_tasks_sent++;
    policy_state->round_robin_index += 1;
    policy_state->round_robin_index %= utarray_len(state->local_schedulers);
    assign_task_to_local_scheduler(state, task, scheduler->id);
  } else {
    CHECKM(0, "No local schedulers. We currently don't handle this case.");
  }
}

object_size_entry *create_object_size_hashmap(global_scheduler_state *state,
                                              task_spec *task_spec,
                                              bool *has_args_by_ref) {
  object_size_entry *s = NULL, *object_size_table = NULL;

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
    /* Look up this object id in GS' object cache. */
    scheduler_object_info *obj_info_entry = NULL;
    HASH_FIND(hh, state->scheduler_object_info_table, &obj_id, sizeof(obj_id),
              obj_info_entry);
    if (obj_info_entry == NULL) {
      /* Global scheduler doesn't know anything about this obj_id, warn, skip.*/
      LOG_WARN("Processing task with object id not known to global scheduler");
      continue;
    }
    LOG_DEBUG("[GS] found object id, data_size = %" PRId64,
              obj_info_entry->data_size);
    /* Object is known to the scheduler. For each of its locations, add size. */
    int64_t object_size = obj_info_entry->data_size;
    char **p = NULL;
    char id_string[ID_STRING_SIZE];
    LOG_DEBUG("locations for an arg_by_ref objid = %s",
              object_id_to_string(obj_id, id_string, ID_STRING_SIZE));
    UNUSED(id_string);
    for (p = (char **) utarray_front(obj_info_entry->object_locations);
         p != NULL;
         p = (char **) utarray_next(obj_info_entry->object_locations, p)) {
      const char *object_location = *p;

      LOG_DEBUG("\tobject location: %s", object_location);

      /* look up this location in the local object size hash table */
      HASH_FIND_STR(object_size_table, object_location, s);
      if (NULL == s) {
        /* this location not yet known; add this object location */
        s = calloc(1, sizeof(object_size_entry));
        s->object_location = object_location;
        HASH_ADD_KEYPTR(hh, object_size_table, s->object_location,
                        strlen(s->object_location), s);
      }
      /* At this point the object location exists in our hash table. */
      s->total_object_size += object_size;
    } /* end for each object's location */
  }   /* end for each task's object */

  return object_size_table;
}

void free_object_size_hashmap(object_size_entry *object_size_table) {
  /* destroy local state */
  object_size_entry *tmp, *s = NULL;
  HASH_ITER(hh, object_size_table, s, tmp) {
    HASH_DEL(object_size_table, s);
    /* NOTE: do not free externally stored s->object_location. */
    free(s);
  }
}

db_client_id get_photon_id(global_scheduler_state *state,
                           const char *plasma_location) {
  aux_address_entry *aux_entry = NULL;
  db_client_id photon_id = NIL_ID;
  if (plasma_location != NULL) {
    LOG_DEBUG("max object size location found : %s", plasma_location);
    /* Lookup association of plasma location to photon */
    HASH_FIND_STR(state->plasma_photon_map, plasma_location, aux_entry);
    if (aux_entry) {
      LOG_DEBUG("found photon db client association for plasma ip:port = %s",
                aux_entry->aux_address);
      /* plasma to photon db client id association found, get photon id */
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

  /* Check to make sure this photon_db_client_id matches one of the schedulers*/
  local_scheduler *lsptr = NULL;
  for (lsptr = (local_scheduler *) utarray_front(state->local_schedulers);
       lsptr != NULL; lsptr = (local_scheduler *) utarray_next(
                          state->local_schedulers, lsptr)) {
    if (memcmp(&lsptr->id, &photon_id, sizeof(photon_id)) == 0) {
      LOG_DEBUG("photon_id matched cached local scheduler entry.");
      break;
    }
  }
  if (!lsptr) {
    LOG_WARN("photon_id didn't match any cached local scheduler entries");
  }
  return photon_id;
}

void handle_task_waiting(global_scheduler_state *state,
                         global_scheduler_policy_state *policy_state,
                         task *task) {
  task_spec *task_spec = task_task_spec(task);
  CHECKM(task_spec != NULL,
         "task wait handler encounted a task with NULL spec");
  /* local hash table to keep track of aggregate object sizes per local sched.
   */
  object_size_entry *tmp, *s = NULL, *object_size_table = NULL;
  bool has_args_by_ref = false;

  object_size_table =
      create_object_size_hashmap(state, task_spec, &has_args_by_ref);

  if (!object_size_table) {
    char id_string[ID_STRING_SIZE];
    if (has_args_by_ref) {
      LOG_DEBUG(
          "Using simple policy. Didn't find objects in GS cache for task = %s",
          object_id_to_string(task_task_id(task), id_string, ID_STRING_SIZE));
      /* TODO(future): wait for object notification and try again. */
    } else {
      LOG_DEBUG("Using simple policy. No arguments passed by reference.");
    }
    UNUSED(id_string);
    handle_task_roundrobin(state, policy_state, task);
    return;
  }

  LOG_DEBUG("Using transfer-aware policy");
  /* pick maximum object_size and assign task to that scheduler. */
  int64_t max_object_size = 0;
  const char *max_object_location = NULL;
  HASH_ITER(hh, object_size_table, s, tmp) {
    if (s->total_object_size > max_object_size) {
      max_object_size = s->total_object_size;
      max_object_location = s->object_location;
    }
  }

  db_client_id photon_id = get_photon_id(state, max_object_location);
  CHECKM(!IS_NIL_ID(photon_id),
         "Failed to find an LS: num_args=%" PRId64 " num_returns=%" PRId64 "\n",
         task_num_args(task_spec), task_num_returns(task_spec));

  assign_task_to_local_scheduler(state, task, photon_id);
  free_object_size_hashmap(object_size_table);
}

void handle_object_available(global_scheduler_state *state,
                             global_scheduler_policy_state *policy_state,
                             object_id object_id) {
  /* Do nothing for now. */
}

void handle_local_scheduler_heartbeat(
    global_scheduler_state *state,
    global_scheduler_policy_state *policy_state) {
  /* Do nothing for now. */
}

void handle_new_local_scheduler(global_scheduler_state *state,
                                global_scheduler_policy_state *policy_state,
                                db_client_id db_client_id) {
  local_scheduler local_scheduler;
  memset(&local_scheduler, 0, sizeof(local_scheduler));
  local_scheduler.id = db_client_id;
  local_scheduler.num_tasks_sent = 0;
  utarray_push_back(state->local_schedulers, &local_scheduler);
}
