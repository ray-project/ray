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
    item_list_.emplace_front(key, size);
    item_map_.emplace(key, item_list_.begin());
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

EvictionPolicy::EvictionPolicy(PlasmaStoreInfo *store_info)
  : memory_used_(0), store_info_(store_info), cache_(new LRUCache()) {}

EvictionPolicy::~EvictionPolicy() {
  delete cache_;
}

int64_t EvictionPolicy::choose_objects_to_evict(
    int64_t num_bytes_required,
    std::vector<ObjectID> &objects_to_evict) {
  int64_t bytes_evicted = cache_->choose_objects_to_evict(num_bytes_required, objects_to_evict);
  /* Update the LRU cache. */
  for (auto &object_id : objects_to_evict) {
    cache_->remove(object_id);
  }
  /* Update the number of bytes used. */
  memory_used_ -= bytes_evicted;
  return bytes_evicted;
}

void EvictionPolicy::object_created(ObjectID object_id) {
  auto entry = store_info_->objects[object_id].get();
  cache_->add(object_id, entry->info.data_size + entry->info.metadata_size);
}

bool EvictionPolicy::require_space(int64_t size,
                                   std::vector<ObjectID> &objects_to_evict) {
  /* Check if there is enough space to create the object. */
  int64_t required_space = memory_used_ + size - store_info_->memory_capacity;
  int64_t num_bytes_evicted;
  if (required_space > 0) {
    /* Try to free up at least as much space as we need right now but ideally
     * up to 20% of the total capacity. */
    int64_t space_to_free = MAX(size, store_info_->memory_capacity / 5);
    LOG_DEBUG("not enough space to create this object, so evicting objects");
    /* Choose some objects to evict, and update the return pointers. */
    num_bytes_evicted = choose_objects_to_evict(space_to_free, objects_to_evict);
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
    memory_used_ += size;
  }
  return num_bytes_evicted >= required_space;
}

void EvictionPolicy::begin_object_access(ObjectID object_id,
                                         std::vector<ObjectID> &objects_to_evict) {
  /* If the object is in the LRU cache, remove it. */
  cache_->remove(object_id);
}

void EvictionPolicy::end_object_access(ObjectID object_id,
                                       std::vector<ObjectID> &objects_to_evict) {
  auto entry = store_info_->objects[object_id].get();
  /* Add the object to the LRU cache.*/
  cache_->add(object_id, entry->info.data_size + entry->info.metadata_size);
}
