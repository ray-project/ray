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
 * This method will be called before an object is created in the Plasma store.
 *
 * @param eviction_state The state managed by the eviction policy.
 * @param plasma_store_info Information about the Plasma store that is exposed
 *        to the eviction policy.
 * @param size The size in bytes of the new object, including both data and
 *        metadata.
 * @return Void.
 */
void handle_before_create(eviction_state *eviction_state,
                          plasma_store_info *plasma_store_info,
                          int64_t size);

/**
 * This method will be called whenever remove_client_from_object_clients is
 * called in the Plasma store.
 *
 * @param eviction_state The state managed by the eviction policy.
 * @param plasma_store_info Information about the Plasma store that is exposed
 *        to the eviction policy.
 * @param The object table entry that just had a client added to it.
 * @return Void.
 */
void handle_add_client(eviction_state *eviction_state,
                       plasma_store_info *plasma_store_info,
                       object_table_entry *entry);

/**
 * This method will be called whenever add_client_to_object_clients is called in
 * the Plasma store.
 *
 * @param eviction_state The state managed by the eviction policy.
 * @param plasma_store_info Information about the Plasma store that is exposed
 *        to the eviction policy.
 * @param entry The object table entry that just had a client removed from it.
 * @return Void.
 */
void handle_remove_client(eviction_state *eviction_state,
                          plasma_store_info *plasma_store_info,
                          object_table_entry *entry);

/**
 * Remove the least recently released objects to try to free up some space.
 *
 * @note This method is not part of the API. It is exposed in the header file
 * only for testing.
 *
 * @param eviction_state The state managed by the eviction policy.
 * @param plasma_store_info Information about the Plasma store that is exposed
 *        to the eviction policy.
 * @param num_bytes_required The number of bytes of space to try to free up.
 */
int64_t evict_objects(eviction_state *eviction_state,
                      plasma_store_info *plasma_store_info,
                      int64_t num_bytes_required);

#endif /* EVICTION_POLICY_H */
