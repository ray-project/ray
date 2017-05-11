#ifndef EVICTION_POLICY_H
#define EVICTION_POLICY_H

#include "plasma.h"

/* ==== The eviction policy ====
 *
 * This file contains declaration for all functions and data structures that
 * need to be provided if you want to implement a new eviction algorithm for the
 * Plasma store.
 */

class LRUCache;

/** The eviction policy. */
class EvictionPolicy {
 public:
   /**
    * Construct an eviction policy.
    *
    * @param store_info Information about the Plasma store that is exposed
    *        to the eviction policy.
    */
  EvictionPolicy(PlasmaStoreInfo *store_info);

  ~EvictionPolicy();

  /**
   * Choose some objects to evict from the Plasma store. When this method is
   * called, the eviction policy will assume that the objects chosen to be evicted
   * will in fact be evicted from the Plasma store by the caller.
   *
   * @note This method is not part of the API. It is exposed in the header file
   * only for testing.
   *
   * @param num_bytes_required The number of bytes of space to try to free up.
   * @param objects_to_evict The object IDs that were chosen for eviction will be
   *        stored into this vector.
   * @return The total number of bytes of space chosen to be evicted.
   */
  int64_t choose_objects_to_evict(int64_t num_bytes_required, std::vector<ObjectID> &objects_to_evict);

  /**
   * This method will be called whenever an object is first created in order to
   * add it to the LRU cache. This is done so that the first time, the Plasma
   * store calls begin_object_access, we can remove the object from the LRU cache.
   *
   * @param object_id The object ID of the object that was created.
   * @return Void.
   */
  void object_created(ObjectID object_id);

  /**
   * This method will be called when the Plasma store needs more space, perhaps to
   * create a new object. If the required amount of space cannot be freed up, then
   * a fatal error will be thrown. When this method is called, the eviction policy
   * will assume that the objects chosen to be evicted will in fact be evicted
   * from the Plasma store by the caller.
   *
   * @param size The size in bytes of the new object, including both data and
   *        metadata.
   * @param objects_to_evict The object IDs that were chosen for eviction will be
   *        stored into this vector.
   * @return True if enough space can be freed and false otherwise.
   */
  bool require_space(int64_t size, std::vector<ObjectID> &objects_to_evict);

  /**
   * This method will be called whenever an unused object in the Plasma store
   * starts to be used. When this method is called, the eviction policy will
   * assume that the objects chosen to be evicted will in fact be evicted from the
   * Plasma store by the caller.
   *
   * @param object_id The ID of the object that is now being used.
   * @param objects_to_evict The object IDs that were chosen for eviction will be
   *        stored into this vector.
   * @return Void.
   */
  void begin_object_access(ObjectID object_id, std::vector<ObjectID> &objects_to_evict);

  /**
   * This method will be called whenever an object in the Plasma store that was
   * being used is no longer being used. When this method is called, the eviction
   * policy will assume that the objects chosen to be evicted will in fact be
   * evicted from the Plasma store by the caller.
   *
   * @param object_id The ID of the object that is no longer being used.
   * @param objects_to_evict The object IDs that were chosen for eviction will be
   *        stored into this vector.
   * @return Void.
   */
  void end_object_access(ObjectID object_id, std::vector<ObjectID> &objects_to_evict);

 private:
  /** Pointer to the plasma store info. */
  PlasmaStoreInfo *store_info_;
  /** The amount of memory (in bytes) currently being used. */
  int64_t memory_used_;
  /** Datastructure for the LRU cache. */
  LRUCache *cache_;
};

#endif /* EVICTION_POLICY_H */
