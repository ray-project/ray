#include "eviction_policy.h"

#include <list>
#include <unordered_map>

class LRUCache {
 private:
  /** A doubly-linked list containing the items in the cache and
   *  their sizes in LRU order. */
  typedef std::list<std::pair<ObjectID, int64_t>> ItemList;
  ItemList item_list_;
  /** A hash table mapping the object ID of an object in the cache to its
   *  location in the doubly linked list item_list_. */
  std::unordered_map<ObjectID, ItemList::iterator, UniqueIDHasher> item_map_;

 public:
  LRUCache(){};

  void add(const ObjectID &key, int64_t size) {
    auto it = item_map_.find(key);
    CHECK(it == item_map_.end());
    item_list_.push_front(std::make_pair(key, size));
    item_map_.insert(std::make_pair(key, item_list_.begin()));
  }

  void remove(const ObjectID &key) {
    auto it = item_map_.find(key);
    CHECK(it != item_map_.end());
    item_list_.erase(it->second);
    item_map_.erase(it);
  }

  int64_t choose_objects_to_evict(int64_t num_bytes_required,
                                  std::vector<ObjectID> &objects_to_evict) {
    int64_t bytes_evicted = 0;
    auto it = item_list_.end();
    while (bytes_evicted < num_bytes_required && it != item_list_.begin()) {
      it--;
      objects_to_evict.push_back(it->first);
      bytes_evicted += it->second;
    }
    return bytes_evicted;
  }
};

/** The part of the Plasma state that is maintained by the eviction policy. */
struct EvictionState {
  /** The amount of memory (in bytes) currently being used. */
  int64_t memory_used;
  /** Datastructure for the LRU cache. */
  LRUCache cache;
};

EvictionState *EvictionState_init() {
  EvictionState *s = new EvictionState();
  s->memory_used = 0;
  return s;
}

void EvictionState_free(EvictionState *s) {
  delete s;
}

int64_t EvictionState_choose_objects_to_evict(
    EvictionState *eviction_state,
    PlasmaStoreInfo *plasma_store_info,
    int64_t num_bytes_required,
    std::vector<ObjectID> &objects_to_evict) {
  int64_t bytes_evicted = eviction_state->cache.choose_objects_to_evict(num_bytes_required, objects_to_evict);
  /* Update the LRU cache. */
  for (auto &object_id : objects_to_evict) {
    eviction_state->cache.remove(object_id);
  }
  /* Construct the return values. */
  *num_objects_to_evict = objs_to_evict.size();
  if (objs_to_evict.size() == 0) {
    *objects_to_evict = NULL;
  } else {
    int64_t result_size = objs_to_evict.size() * sizeof(ObjectID);
    *objects_to_evict = (ObjectID *) malloc(result_size);
    memcpy(*objects_to_evict, objs_to_evict.data(), result_size);
  }
  /* Update the number of bytes used. */
  eviction_state->memory_used -= bytes_evicted;
  return bytes_evicted;
}

void EvictionState_object_created(EvictionState *eviction_state,
                                  PlasmaStoreInfo *plasma_store_info,
                                  ObjectID object_id) {
  ObjectTableEntry *entry = plasma_store_info->objects[object_id];
  eviction_state->cache.add(object_id,
                            entry->info.data_size + entry->info.metadata_size);
}

bool EvictionState_require_space(EvictionState *eviction_state,
                                 PlasmaStoreInfo *plasma_store_info,
                                 int64_t size,
                                 std::vector<ObjectID> &objects_to_evict) {
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
        eviction_state, plasma_store_info, space_to_free, objects_to_evict);
    LOG_INFO(
        "There is not enough space to create this object, so evicting "
        "%" PRId64 " objects to free up %" PRId64 " bytes.",
        objects_to_evict.size(), num_bytes_evicted);
  } else {
    num_bytes_evicted = 0;
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
                                       ObjectID object_id,
                                       std::vector<ObjectID> &objects_to_evict) {
  /* If the object is in the LRU cache, remove it. */
  eviction_state->cache.remove(object_id);
}

void EvictionState_end_object_access(EvictionState *eviction_state,
                                     PlasmaStoreInfo *plasma_store_info,
                                     ObjectID object_id,
                                     std::vector<ObjectID> &objects_to_evict) {
  auto entry = plasma_store_info->objects[object_id];
  /* Add the object to the LRU cache.*/
  eviction_state->cache.add(object_id,
                            entry->info.data_size + entry->info.metadata_size);
}
