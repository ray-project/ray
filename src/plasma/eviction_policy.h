#ifndef EVICTION_POLICY_H
#define EVICTION_POLICY_H

#include "plasma.h"

/* ==== The eviction policy ====
 *
 * This file contains declaration for all functions and data structures that
 * need to be provided if you want to implement a new eviction algorithm for the
 * Plasma store.
 */

/** Internal state of the eviction policy. */
typedef struct eviction_state eviction_state;

/**
 * Initialize the eviction policy state.
 *
 * @return The internal state of the eviction policy.
 */
eviction_state *make_eviction_state(void);

/**
 * Free the eviction policy state.
 *
 * @param state The state managed by the eviction policy.
 * @return Void.
 */
void free_eviction_state(eviction_state *state);

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
 * @return Void.
 */
void require_space(eviction_state *eviction_state,
                   plasma_store_info *plasma_store_info,
                   int64_t size,
                   int64_t *num_objects_to_evict,
                   object_id **objects_to_evict);

/**
 * This method will be called whenever an unused object in the Plasma store
 * starts to be used. When this method is called, the eviction policy will
 * assume that the objects chosen to be evicted will in fact be evicted from the
 * Plasma store by the caller.
 *
 * @param eviction_state The state managed by the eviction policy.
 * @param plasma_store_info Information about the Plasma store that is exposed
 *        to the eviction policy.
 * @param The object table entry that just had a client added to it.
 * @param num_objects_to_evict The number of objects that are chosen will be
 *        stored at this address.
 * @param objects_to_evict An array of the object IDs that were chosen will be
 *        stored at this address. If the number of objects chosen is greater
 *        than 0, then the caller needs to free that array. If it equals 0, then
 *        the array will be NULL.
 * @return Void.
 */
void begin_object_access(eviction_state *eviction_state,
                         plasma_store_info *plasma_store_info,
                         object_id obj_id,
                         int64_t *num_objects_to_evict,
                         object_id **objects_to_evict);

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
void end_object_access(eviction_state *eviction_state,
                       plasma_store_info *plasma_store_info,
                       object_id obj_id,
                       int64_t *num_objects_to_evict,
                       object_id **objects_to_evict);

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
int64_t choose_objects_to_evict(eviction_state *eviction_state,
                                plasma_store_info *plasma_store_info,
                                int64_t num_bytes_required,
                                int64_t *num_objects_to_evict,
                                object_id **objects_to_evict);

#endif /* EVICTION_POLICY_H */
