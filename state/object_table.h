#include "common.h"
#include "db.h"

/* The callback that is called when the result of a lookup
 * in the object table comes back. The callback should free
 * the manager_vector array, but NOT the strings they are pointing to. */
typedef void (*lookup_callback)(object_id object_id,
                                int manager_count,
                                const char *manager_vector[],
                                void *context);

/* Register a new object with the directory. */
/* TODO(pcm): Retry, print for each attempt. */
void object_table_add(db_handle *db, object_id object_id);

/* Remove object from the directory. */
void object_table_remove(db_handle *db,
                         object_id object_id,
                         const char *manager);

/* Look up entry from the directory */
void object_table_lookup(db_handle *db,
                         object_id object_id,
                         lookup_callback callback,
                         void *context);
