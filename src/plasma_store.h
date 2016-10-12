#ifndef PLASMA_STORE_H
#define PLASMA_STORE_H

#include "plasma.h"

typedef struct plasma_store_state plasma_store_state;

/**
 * Create a new object:
 *
 * @param s The plasma store state.
 * @param object_id Object ID of the object to be created.
 * @param data_size Size in bytes of the object to be created.
 * @param metadata_size Size in bytes of the object metadata.
 * @return The new plasma object.
 */
plasma_object create_object(plasma_store_state *s,
                            object_id object_id,
                            int64_t data_size,
                            int64_t metadata_size,
                            plasma_object *result);

/**
 * Get an object:
 *
 * @param s The plasma store state.
 * @param conn The client connection that requests the object.
 * @param object_id Object ID of the object to be gotten.
 * @return The status of the object (object_status in plasma.h).
 */
int get_object(plasma_store_state *s,
               int conn,
               object_id object_id,
               plasma_object *result);

/**
 * Seal an object:
 *
 * @param s The plasma store state.
 * @param object_id Object ID of the object to be sealed.
 * @param conns Returns the connection that are waiting for this object.
                The caller is responsible for destroying this array.
 * @return Void.
 */
void seal_object(plasma_store_state *s,
                 object_id object_id,
                 UT_array **conns,
                 plasma_object *result);

/**
 * Check if the plasma store contains an object:
 *
 * @param s The plasma store state.
 * @param object_id Object ID that will be checked.
 * @return OBJECT_FOUND if the object is in the store, OBJECT_NOT_FOUND if not
 */
int contains_object(plasma_store_state *s, object_id object_id);

/**
 * Delete an object from the plasma store:
 *
 * @param s The plasma store state.
 * @param object_id Object ID of the object to be deleted.
 * @return Void.
 */
void delete_object(plasma_store_state *s, object_id object_id);

#endif /* PLASMA_STORE_H */
