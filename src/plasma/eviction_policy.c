#include "eviction_policy.h"

void dlfree(void *);

/** The part of the Plasma state that is maintained by the eviction policy. */
struct eviction_state {
  /** The amount of memory (in bytes) that we allow to be allocated in this
   *  store. */
  int64_t memory_capacity;
  /** The amount of memory (in bytes) currently being used. */
  int64_t memory_used;
  /** An array of the released objects in order from least recently released to
   *  most recently released. */
  UT_array *released_objects;
};

/* This is used to define the array of object IDs used to define the
 * released_objects type. */
UT_icd released_objects_entry_icd = {sizeof(object_id), NULL, NULL, NULL};

eviction_state *make_eviction_state(void) {
  eviction_state *state = malloc(sizeof(eviction_state));
  /* Find the amount of available memory on the machine. */
  state->memory_capacity = 1000000000;
  state->memory_used = 0;
  utarray_new(state->released_objects, &released_objects_entry_icd);
  return state;
}

void free_eviction_state(eviction_state *s) {
  utarray_free(s->released_objects);
  free(s);
}

void delete_obj(eviction_state *eviction_state,
                plasma_store_info *plasma_store_info,
                object_id object_id) {
  LOG_DEBUG("deleting object");
  object_table_entry *entry;
  HASH_FIND(handle, plasma_store_info->sealed_objects, &object_id,
            sizeof(object_id), entry);
  /* TODO(rkn): This should probably not fail, but should instead throw an
   * error. Maybe we should also support deleting objects that have been created
   * but not sealed. */
  CHECKM(entry != NULL, "To delete an object it must have been sealed.");
  CHECKM(utarray_len(entry->clients) == 0,
         "To delete an object, there must be no clients currently using it.");
  uint8_t *pointer = entry->pointer;
  HASH_DELETE(handle, plasma_store_info->sealed_objects, entry);
  dlfree(pointer);
  utarray_free(entry->clients);
  free(entry);
}

/* Remove the least recently released objects. */
int64_t evict_objects(eviction_state *eviction_state,
                      plasma_store_info *plasma_store_info,
                      int64_t num_bytes_required) {
  int num_objects_evicted = 0;
  int64_t num_bytes_evicted = 0;
  for (int i = 0; i < utarray_len(eviction_state->released_objects); ++i) {
    if (num_bytes_evicted >= num_bytes_required) {
      break;
    }
    object_id *obj_id =
        (object_id *) utarray_eltptr(eviction_state->released_objects, i);
    object_table_entry *entry;
    HASH_FIND(handle, plasma_store_info->open_objects, obj_id, sizeof(object_id),
              entry);
    if (entry == NULL) {
      HASH_FIND(handle, plasma_store_info->sealed_objects, obj_id, sizeof(object_id),
                entry);
    }
    num_objects_evicted += 1;
    num_bytes_evicted += (entry->info.data_size + entry->info.metadata_size);
    delete_obj(eviction_state, plasma_store_info, *obj_id);
  }
  /* Remove the deleted objects from the released objects. */
  utarray_erase(eviction_state->released_objects, 0, num_objects_evicted);
  eviction_state->memory_used -= num_bytes_evicted;
  return num_bytes_evicted;
}

void handle_before_create(eviction_state *eviction_state,
                          plasma_store_info *plasma_store_info,
                          int64_t size) {
  /* Check if there is enough space to create the object. */
  int64_t required_space = eviction_state->memory_used + size -
                           eviction_state->memory_capacity;
  if (required_space > 0) {
    /* Try to free up as much free space as we need right now. */
    LOG_DEBUG("not enough space to create this object, so evicting objects");
    printf("There is not enough space to create this object, so evicting objects.\n");
    int64_t num_bytes_evicted = evict_objects(eviction_state, plasma_store_info,
                                              required_space);
    printf("Evicted %lld bytes.\n", num_bytes_evicted);
    CHECK(num_bytes_evicted >= required_space);
  }
  eviction_state->memory_used += size;
}

void handle_add_client(eviction_state *eviction_state,
                       plasma_store_info *plasma_store_info,
                       object_table_entry *entry) {
  /* If the object was previously unused, remove the object from the list of
   * released objects. */
  /* TODO(rkn): This is extremely slow. It can be made more efficient with
   * better data structures. */
  if (utarray_len(entry->clients) == 0) {
    for (int i = 0; i < utarray_len(eviction_state->released_objects); ++i) {
      object_id *obj_id =
          (object_id *) utarray_eltptr(eviction_state->released_objects, i);
      if (memcmp(obj_id, &entry->object_id, sizeof(object_id)) == 0) {
        utarray_erase(eviction_state->released_objects, i, 1);
        break;
      }
    }
    /* TODO(rkn): It'd be nice to check that something was actually removed, but
     * the first time we call add_client_to_object_clients, it won't be in the
     * list. */
  }
}

void handle_remove_client(eviction_state *eviction_state,
                          plasma_store_info *plasma_store_info,
                          object_table_entry *entry) {
  /* If no more clients are using this object, add the object to the list of
   * released objects. */
  if (utarray_len(entry->clients) == 0) {
    utarray_push_back(eviction_state->released_objects, &entry->object_id);
  }
}

void handle_delete(eviction_state *eviction_state,
                   plasma_store_info *plasma_store_info,
                   object_id object_id) {
  delete_obj(eviction_state, plasma_store_info, object_id);
}
