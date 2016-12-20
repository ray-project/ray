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

void handle_task_nodep(global_scheduler_state *state,
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

void handle_task_waiting(global_scheduler_state *state,
                         global_scheduler_policy_state *policy_state,
                         task *task) {

  task_spec *task_spec = task_task_spec(task);
  CHECKM(task_spec != NULL, "task wait handler encounted a task with NULL spec");
  printf("[GS] TASKWAIT: starting to iterate %lld task args\n", task_num_args(task_spec));
  /* local hash table to keep track of aggregate object sizes per local sched.
   */
  handle_task_nodep(state, policy_state, task);
  return;
  struct object_size_entry {
    const char *object_location;
    int64_t total_object_size;
    UT_hash_handle hh;
  } *tmp, *s = NULL, *object_size_table = NULL;

  /* If none of the args are passed by reference, handle task as having no dep.*/
  bool args_by_ref = false;
  for (int i = 0; i < task_num_args(task_spec); i++) {
    /* Object ids are only available for args by references.
     * Args by value are serialized into the task_spec itself.
     * We will only concern ourselves with args by ref for data size calculation
     */
    if (task_arg_type(task_spec, i) != ARG_BY_REF) {
      continue;
    }
    args_by_ref = true;
    object_id obj_id = task_arg_id(task_spec, i);
    /* Given this object id, look it up.
     */
    scheduler_object_info *obj_info_entry = NULL;
    HASH_FIND(hh, state->scheduler_object_info_table, &obj_id, sizeof(obj_id),
        obj_info_entry);
    if (obj_info_entry == NULL) {
      /* Global scheduler doesn't know anything about this obj_id, warn, skip.*/
      LOG_WARN("Processing task with object id not known to global scheduler");
      printf("[GS] Processing task with object id not known to global scheduler\n");
      continue;
    }
    printf("[GS] found object id, data_size=%lld \n", obj_info_entry->data_size);
    object_id_print(obj_info_entry->object_id);
    /* Object is known to the scheduler. For each of its locations, add size.
     */
    int64_t object_size = obj_info_entry->data_size;
    char **p = NULL;
    printf("printing all locations for an arg_by_ref ");object_id_print(obj_id);
    fflush(stdout);
    for (p = (char **)utarray_front(obj_info_entry->object_locations);
        p != NULL;
        p = (char **)utarray_next(obj_info_entry->object_locations, p)) {
    //while (p = (char **)utarray_next(obj_info_entry->object_locations, p)) {
    //for (int j = 0; j < utarray_len(obj_info_entry->object_locations); j++) {
      const char *object_location = *p;

      printf("\tobject location: %s\n", object_location);

      /* look up this location in the local object size hash table */
      HASH_FIND_STR(object_size_table, object_location, s);
      if (NULL == s) {
        /* this location not yet known; add this object location */
        s = calloc(1, sizeof(struct object_size_entry));
        s->object_location = object_location;
        HASH_ADD_KEYPTR(hh, object_size_table, s->object_location,
            strlen(s->object_location), s);
      }
      /* At this point the object location exists in our hash table. */
      s->total_object_size += object_size;
    } /* end for each object's location */
  } /* end for each task's object */

  if (!args_by_ref) {
    /* All args were by value or no args at all. Fall back to simple policy. */
    printf("[GS]: fall back to simple policy\n");
    handle_task_nodep(state, policy_state, task);
    return;
  }

  /* pick maximum object_size and assign task to that scheduler.
   */
  int64_t max_object_size = 0;
  const char * max_object_location = NULL;
  HASH_ITER(hh, object_size_table, s, tmp) {
    if (s->total_object_size > max_object_size) {
      max_object_size = s->total_object_size;
      max_object_location = s->object_location;
    }
  }
  /* XXXX CONTINUE HERE ; if location found, look up association with photon and assign*/
  aux_address_entry *aux_entry = NULL;
  db_client_id photon_id = NIL_ID;
  if (max_object_location != NULL) {
    printf("max object size location found!");
    LOG_INFO("max object size location found : %s\n", max_object_location);
    /* Lookup association of plasma location to photon */
    HASH_FIND_STR(state->plasma_photon_map, max_object_location, aux_entry);
    if (aux_entry) {
      /* plasma to photon db client id association found, get photon id */
      photon_id = aux_entry->photon_db_client_id;
    }
  }

  printf("[GS] photon ID found = "); object_id_print(photon_id);
  CHECKM(!IS_NIL_ID(photon_id),
         "GS failed to find an LS: num_args=%lld num_returns=%lld\n",
         task_num_args(task_spec),
         task_num_returns(task_spec));

  assign_task_to_local_scheduler(state, task, photon_id);

  /* destroy local state */
  HASH_ITER(hh, object_size_table, s, tmp) {
    HASH_DEL(object_size_table, s);
    /* NOTE: do not free externally stored s->object_location */
    free(s);
  }
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
