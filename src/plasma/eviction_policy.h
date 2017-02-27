#ifndef EVICTION_POLICY_H
#define EVICTION_POLICY_H

#include "plasma.h"

#ifdef __cplusplus
extern "C" {
#endif

/* ==== The eviction policy ====
 *
 * This file contains declaration for all functions and data structures that
 * need to be provided if you want to implement a new eviction algorithm for the
 * Plasma store.
 */

/** Internal state of the eviction policy. */
typedef struct EvictionState EvictionState;

/**
 * Initialize the eviction policy state.
 *
 * @param system_memory The amount of memory that can be used by the Plasma
 *        store.
 * @return The internal state of the eviction policy.
 */
EvictionState *EvictionState_init(void);

/**
 * Free the eviction policy state.
 *
 * @param state The state managed by the eviction policy.
 * @return Void.
 */
void EvictionState_free(EvictionState *state);

/**
 * This method will be called whenever an object is first created in order to
 * add it to the LRU cache. This is done so that the first time, the Plasma
 * store calls begin_object_access, we can remove the object from the LRU cache.
 *
 * @param eviction_state The state managed by the eviction policy.
 * @param plasma_store_info Information about the Plasma store that is exposed
 *        to the eviction policy.
 * @param obj_id The object ID of the object that was created.
 * @return Void.
 */
void EvictionState_object_created(EvictionState *eviction_state,
                                  PlasmaStoreInfo *plasma_store_info,
                                  ObjectID obj_id);

/**
 * This method will be called when the Plasma store needs more space, perhaps to
 * create a new object. If the required amount of space cannot be freed up, then
 * a fatal error will be thrown. When this method is called, the eviction policy
 * will assume that the objects chosen to be evicted will in fact be evicted
 * from the Plasma store by the caller.
 *
 * @param eviction_state The state managed by the eviction policy.
 * @param plasma_store_info Information about the Plasma store that is exposed
 *        to the eviction policy.
 * @param size The size in bytes of the new object, including both data and
 *        metadata.
 * @param num_objects_to_evict The number of objects that are chosen will be
 *        stored at this address.
 * @param objects_to_evict An array of the object IDs that were chosen will be
 *        stored at this address. If the number of objects chosen is greater
 *        than 0, then the caller needs to free that array. If it equals 0, then
 *        the array will be NULL.
 * @return True if enough space can be freed and false otherwise.
 */
bool EvictionState_require_space(EvictionState *eviction_state,
                                 PlasmaStoreInfo *plasma_store_info,
                                 int64_t size,
                                 int64_t *num_objects_to_evict,
                                 ObjectID **objects_to_evict);

/**
 * This method will be called whenever an unused object in the Plasma store
 * starts to be used. When this method is called, the eviction policy will
 * assume that the objects chosen to be evicted will in fact be evicted from the
 * Plasma store by the caller.
 *
 * @param eviction_state The state managed by the eviction policy.
 * @param plasma_store_info Information about the Plasma store that is exposed
 *        to the eviction policy.
 * @param obj_id The ID of the object that is now being used.
 * @param num_objects_to_evict The number of objects that are chosen will be
 *        stored at this address.
 * @param objects_to_evict An array of the object IDs that were chosen will be
 *        stored at this address. If the number of objects chosen is greater
 *        than 0, then the caller needs to free that array. If it equals 0, then
 *        the array will be NULL.
 * @return Void.
 */
void EvictionState_begin_object_access(EvictionState *eviction_state,
                                       PlasmaStoreInfo *plasma_store_info,
                                       ObjectID obj_id,
                                       int64_t *num_objects_to_evict,
                                       ObjectID **objects_to_evict);

/**
 * This method will be called whenever an object in the Plasma store that was
 * being used is no longer being used. When this method is called, the eviction
 * policy will assume that the objects chosen to be evicted will in fact be
 * evicted from the Plasma store by the caller.
 *
 * @param eviction_state The state managed by the eviction policy.
 * @param plasma_store_info Information about the Plasma store that is exposed
 *        to the eviction policy.
 * @param obj_id The ID of the object that is no longer being used.
 * @param num_objects_to_evict The number of objects that are chosen will be
 *        stored at this address.
 * @param objects_to_evict An array of the object IDs that were chosen will be
 *        stored at this address. If the number of objects chosen is greater
 *        than 0, then the caller needs to free that array. If it equals 0, then
 *        the array will be NULL.
 * @return Void.
 */
void EvictionState_end_object_access(EvictionState *eviction_state,
                                     PlasmaStoreInfo *plasma_store_info,
                                     ObjectID obj_id,
                                     int64_t *num_objects_to_evict,
                                     ObjectID **objects_to_evict);

/**
 * Choose some objects to evict from the Plasma store. When this method is
 * called, the eviction policy will assume that the objects chosen to be evicted
 * will in fact be evicted from the Plasma store by the caller.
 *
 * @note This method is not part of the API. It is exposed in the header file
 * only for testing.
 *
 * @param eviction_state The state managed by the eviction policy.
 * @param plasma_store_info Information about the Plasma store that is exposed
 *        to the eviction policy.
 * @param num_bytes_required The number of bytes of space to try to free up.
 * @param num_objects_to_evict The number of objects that are chosen will be
 *        stored at this address.
 * @param objects_to_evict An array of the object IDs that were chosen will be
 *        stored at this address. If the number of objects chosen is greater
 *        than 0, then the caller needs to free that array. If it equals 0, then
 *        the array will be NULL.
 * @return The total number of bytes of space chosen to be evicted.
 */
int64_t EvictionState_choose_objects_to_evict(
    EvictionState *eviction_state,
    PlasmaStoreInfo *plasma_store_info,
    int64_t num_bytes_required,
    int64_t *num_objects_to_evict,
    ObjectID **objects_to_evict);

#ifdef __cplusplus
}
#endif

#endif /* EVICTION_POLICY_H */
