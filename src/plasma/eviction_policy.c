#include "eviction_policy.h"

#include "utlist.h"

void dlfree(void *);

/** An element representing a released object in a doubly-linked list. This is
 *  used to implement an LRU cache. */
typedef struct released_object {
  /** The object_id of the released object. */
  object_id object_id;
  /** Needed for the doubly-linked list macros. */
  struct released_object *prev;
  /** Needed for the doubly-linked list macros. */
  struct released_object *next;
} released_object;

/** This type is used to define a hash table mapping the object ID of a released
 *  object to its location in the doubly-linked list of released objects. */
typedef struct {
  /** Object ID of this object. */
  object_id object_id;
  /** A pointer to the corresponding entry for this object in the doubly-linked
   *  list of released objects. */
  released_object *released_object;
  /** Handle for the uthash table. */
  UT_hash_handle handle;
} released_object_entry;

/** The part of the Plasma state that is maintained by the eviction policy. */
struct eviction_state {
  /** The amount of memory (in bytes) that we allow to be allocated in this
   *  store. */
  int64_t memory_capacity;
  /** The amount of memory (in bytes) currently being used. */
  int64_t memory_used;
  /** A doubly-linked list of the released objects in order from least recently
   *  released to most recently released. */
  released_object *released_objects;
  /** A hash table mapping the object ID of a released object to its location in
   *  the doubly linked list of released objects. */
  released_object_entry *released_object_table;
};

/* This is used to define the array of object IDs used to define the
 * released_objects type. */
UT_icd released_objects_entry_icd = {sizeof(object_id), NULL, NULL, NULL};

eviction_state *make_eviction_state(void) {
  eviction_state *state = malloc(sizeof(eviction_state));
  /* Find the amount of available memory on the machine. */
  state->memory_capacity = 1000000000;
  state->memory_used = 0;
  state->released_objects = NULL;
  state->released_object_table = NULL;
  return state;
}

void free_eviction_state(eviction_state *s) {
  /* Delete each element in the doubly-linked list. */
  released_object *element, *temp;
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

void add_object_to_lru_cache(eviction_state *eviction_state,
                             object_id object_id) {
  /* Add the object ID to the doubly-linked list. */
  released_object *linked_list_entry = malloc(sizeof(released_object));
  linked_list_entry->object_id = object_id;
  DL_APPEND(eviction_state->released_objects, linked_list_entry);
  /* Check that the object ID is not already in the hash table. */
  released_object_entry *hash_table_entry;
  HASH_FIND(handle, eviction_state->released_object_table, &object_id,
            sizeof(object_id), hash_table_entry);
  CHECK(hash_table_entry == NULL);
  /* Add the object ID to the hash table. */
  hash_table_entry = malloc(sizeof(released_object_entry));
  hash_table_entry->object_id = object_id;
  hash_table_entry->released_object = linked_list_entry;
  HASH_ADD(handle, eviction_state->released_object_table, object_id,
           sizeof(object_id), hash_table_entry);
}

void remove_object_from_lru_cache(eviction_state *eviction_state,
                                  object_id object_id) {
  /* Check that the object ID is in the hash table. */
  released_object_entry *hash_table_entry;
  HASH_FIND(handle, eviction_state->released_object_table, &object_id,
            sizeof(object_id), hash_table_entry);
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

void delete_obj(eviction_state *eviction_state,
                plasma_store_info *plasma_store_info,
                object_id object_id) {
  LOG_DEBUG("deleting object");
  object_table_entry *entry;
  HASH_FIND(handle, plasma_store_info->objects, &object_id, sizeof(object_id),
            entry);
  /* TODO(rkn): This should probably not fail, but should instead throw an
   * error. Maybe we should also support deleting objects that have been created
   * but not sealed. */
  CHECKM(entry != NULL, "To delete an object it must be in the object table.");
  CHECKM(entry->state == SEALED,
         "To delete an object it must have been sealed.")
  CHECKM(utarray_len(entry->clients) == 0,
         "To delete an object, there must be no clients currently using it.");
  uint8_t *pointer = entry->pointer;
  HASH_DELETE(handle, plasma_store_info->objects, entry);
  dlfree(pointer);
  utarray_free(entry->clients);
  free(entry);
}

/* Remove the least recently released objects. */
int64_t evict_objects(eviction_state *eviction_state,
                      plasma_store_info *plasma_store_info,
                      int64_t num_bytes_required) {
  int64_t num_bytes_evicted = 0;
  /* Evict some objects starting with the least recently released object. */
  released_object *element, *temp;
  DL_FOREACH_SAFE(eviction_state->released_objects, element, temp) {
    if (num_bytes_evicted >= num_bytes_required) {
      break;
    }
    object_id obj_id = element->object_id;
    /* Find the object table entry for this object. */
    object_table_entry *entry;
    HASH_FIND(handle, plasma_store_info->objects, &obj_id, sizeof(object_id),
              entry);
    num_bytes_evicted += (entry->info.data_size + entry->info.metadata_size);
    /* Delete the object. */
    delete_obj(eviction_state, plasma_store_info, obj_id);
    /* Update the LRU cache. */
    remove_object_from_lru_cache(eviction_state, obj_id);
  }
  /* Update the number used. */
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
  /* If the object is in the LRU cache, remove it. */
  released_object_entry *hash_table_entry;
  HASH_FIND(handle, eviction_state->released_object_table, &entry->object_id,
            sizeof(object_id), hash_table_entry);
  if (hash_table_entry != NULL) {
    remove_object_from_lru_cache(eviction_state, entry->object_id);
  }
}

void handle_remove_client(eviction_state *eviction_state,
                          plasma_store_info *plasma_store_info,
                          object_table_entry *entry) {
  /* If no more clients are using this object, add it to the LRU cache. */
  if (utarray_len(entry->clients) == 0) {
    add_object_to_lru_cache(eviction_state, entry->object_id);
  }
}

void handle_delete(eviction_state *eviction_state,
                   plasma_store_info *plasma_store_info,
                   object_id object_id) {
  delete_obj(eviction_state, plasma_store_info, object_id);
}
