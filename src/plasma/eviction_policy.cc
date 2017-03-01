#include "eviction_policy.h"

#include "utlist.h"

/** An element representing a released object in a doubly-linked list. This is
 *  used to implement an LRU cache. */
typedef struct ReleasedObject {
  /** The object_id of the released object. */
  ObjectID object_id;
  /** Needed for the doubly-linked list macros. */
  struct ReleasedObject *prev;
  /** Needed for the doubly-linked list macros. */
  struct ReleasedObject *next;
} ReleasedObject;

/** This type is used to define a hash table mapping the object ID of a released
 *  object to its location in the doubly-linked list of released objects. */
typedef struct {
  /** Object ID of this object. */
  ObjectID object_id;
  /** A pointer to the corresponding entry for this object in the doubly-linked
   *  list of released objects. */
  ReleasedObject *released_object;
  /** Handle for the uthash table. */
  UT_hash_handle handle;
} released_object_entry;

/** The part of the Plasma state that is maintained by the eviction policy. */
struct EvictionState {
  /** The amount of memory (in bytes) currently being used. */
  int64_t memory_used;
  /** A doubly-linked list of the released objects in order from least recently
   *  released to most recently released. */
  ReleasedObject *released_objects;
  /** A hash table mapping the object ID of a released object to its location in
   *  the doubly linked list of released objects. */
  released_object_entry *released_object_table;
};

/* This is used to define the array of object IDs used to define the
 * released_objects type. */
UT_icd released_objects_entry_icd = {sizeof(ObjectID), NULL, NULL, NULL};

EvictionState *EvictionState_init(void) {
  EvictionState *state = (EvictionState *) malloc(sizeof(EvictionState));
  state->memory_used = 0;
  state->released_objects = NULL;
  state->released_object_table = NULL;
  return state;
}

void EvictionState_free(EvictionState *s) {
  /* Delete each element in the doubly-linked list. */
  ReleasedObject *element, *temp;
  DL_FOREACH_SAFE(s->released_objects, element, temp) {
    DL_DELETE(s->released_objects, element);
    free(element);
  }
  /* Delete each element in the hash table. */
  released_object_entry *current_entry, *temp_entry;
  HASH_ITER(handle, s->released_object_table, current_entry, temp_entry) {
    HASH_DELETE(handle, s->released_object_table, current_entry);
    free(current_entry);
  }
  /* Free the eviction state. */
  free(s);
}

void add_object_to_lru_cache(EvictionState *eviction_state,
                             ObjectID object_id) {
  /* Add the object ID to the doubly-linked list. */
  ReleasedObject *linked_list_entry = (ReleasedObject *) malloc(sizeof(ReleasedObject));
  linked_list_entry->object_id = object_id;
  DL_APPEND(eviction_state->released_objects, linked_list_entry);
  /* Check that the object ID is not already in the hash table. */
  released_object_entry *hash_table_entry;
  HASH_FIND(handle, eviction_state->released_object_table, &object_id,
            sizeof(object_id), hash_table_entry);
  CHECK(hash_table_entry == NULL);
  /* Add the object ID to the hash table. */
  hash_table_entry = (released_object_entry *) malloc(sizeof(released_object_entry));
  hash_table_entry->object_id = object_id;
  hash_table_entry->released_object = linked_list_entry;
  HASH_ADD(handle, eviction_state->released_object_table, object_id,
           sizeof(object_id), hash_table_entry);
}

void remove_object_from_lru_cache(EvictionState *eviction_state,
                                  ObjectID object_id) {
  /* Check that the object ID is in the hash table. */
  released_object_entry *hash_table_entry;
  HASH_FIND(handle, eviction_state->released_object_table, &object_id,
            sizeof(object_id), hash_table_entry);
  /* Only remove the object ID if it is in the LRU cache. */
  CHECK(hash_table_entry != NULL);
  /* Remove the object ID from the doubly-linked list. */
  DL_DELETE(eviction_state->released_objects,
            hash_table_entry->released_object);
  /* Free the entry from the doubly-linked list. */
  free(hash_table_entry->released_object);
  /* Remove the object ID from the hash table. */
  HASH_DELETE(handle, eviction_state->released_object_table, hash_table_entry);
  /* Free the entry from the hash table. */
  free(hash_table_entry);
}

int64_t EvictionState_choose_objects_to_evict(
    EvictionState *eviction_state,
    PlasmaStoreInfo *plasma_store_info,
    int64_t num_bytes_required,
    int64_t *num_objects_to_evict,
    ObjectID **objects_to_evict) {
  int64_t num_objects = 0;
  int64_t num_bytes = 0;
  /* Figure out how many objects need to be evicted in order to recover a
   * sufficient number of bytes. */
  ReleasedObject *element, *temp;
  DL_FOREACH_SAFE(eviction_state->released_objects, element, temp) {
    if (num_bytes >= num_bytes_required) {
      break;
    }
    /* Find the object table entry for this object. */
    object_table_entry *entry;
    HASH_FIND(handle, plasma_store_info->objects, &element->object_id,
              sizeof(element->object_id), entry);
    /* Update the cumulative bytes and the number of objects so far. */
    num_bytes += (entry->info.data_size + entry->info.metadata_size);
    num_objects += 1;
  }
  /* Construct the return values. */
  *num_objects_to_evict = num_objects;
  if (num_objects == 0) {
    *objects_to_evict = NULL;
  } else {
    *objects_to_evict = (ObjectID *) malloc(num_objects * sizeof(ObjectID));
    int counter = 0;
    DL_FOREACH_SAFE(eviction_state->released_objects, element, temp) {
      if (counter == num_objects) {
        break;
      }
      (*objects_to_evict)[counter] = element->object_id;
      /* Update the LRU cache. */
      remove_object_from_lru_cache(eviction_state, element->object_id);
      counter += 1;
    }
  }
  /* Update the number used. */
  eviction_state->memory_used -= num_bytes;
  return num_bytes;
}

void EvictionState_object_created(EvictionState *eviction_state,
                                  PlasmaStoreInfo *plasma_store_info,
                                  ObjectID obj_id) {
  add_object_to_lru_cache(eviction_state, obj_id);
}

bool EvictionState_require_space(EvictionState *eviction_state,
                                 PlasmaStoreInfo *plasma_store_info,
                                 int64_t size,
                                 int64_t *num_objects_to_evict,
                                 ObjectID **objects_to_evict) {
  /* Check if there is enough space to create the object. */
  int64_t required_space =
      eviction_state->memory_used + size - plasma_store_info->memory_capacity;
  int64_t num_bytes_evicted;
  if (required_space > 0) {
    /* Try to free up at least as much space as we need right now but ideally
     * up to 20% of the total capacity. */
    int64_t space_to_free = MAX(size, plasma_store_info->memory_capacity / 5);
    LOG_DEBUG("not enough space to create this object, so evicting objects");
    /* Choose some objects to evict, and update the return pointers. */
    num_bytes_evicted = EvictionState_choose_objects_to_evict(
        eviction_state, plasma_store_info, space_to_free, num_objects_to_evict,
        objects_to_evict);
    LOG_INFO(
        "There is not enough space to create this object, so evicting "
        "%" PRId64 " objects to free up %" PRId64 " bytes.",
        *num_objects_to_evict, num_bytes_evicted);
  } else {
    num_bytes_evicted = 0;
    *num_objects_to_evict = 0;
    *objects_to_evict = NULL;
  }
  if (num_bytes_evicted >= required_space) {
    /* We only increment the space used if there is enough space to create the
     * object. */
    eviction_state->memory_used += size;
  }
  return num_bytes_evicted >= required_space;
}

void EvictionState_begin_object_access(EvictionState *eviction_state,
                                       PlasmaStoreInfo *plasma_store_info,
                                       ObjectID obj_id,
                                       int64_t *num_objects_to_evict,
                                       ObjectID **objects_to_evict) {
  /* If the object is in the LRU cache, remove it. */
  remove_object_from_lru_cache(eviction_state, obj_id);
  *num_objects_to_evict = 0;
  *objects_to_evict = NULL;
}

void EvictionState_end_object_access(EvictionState *eviction_state,
                                     PlasmaStoreInfo *plasma_store_info,
                                     ObjectID obj_id,
                                     int64_t *num_objects_to_evict,
                                     ObjectID **objects_to_evict) {
  /* Add the object to the LRU cache.*/
  add_object_to_lru_cache(eviction_state, obj_id);
  *num_objects_to_evict = 0;
  *objects_to_evict = NULL;
}
