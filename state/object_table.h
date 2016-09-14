#include "common.h"
#include "db.h"

typedef void (*lookup_callback)(void *);

/* Register a new object with the directory. */
void object_table_add(db_conn *db, unique_id object_id);

/* Remove object from the directory */
void object_table_remove(db_conn *db, unique_id object_id);

/* Look up entry from the directory */
void object_table_lookup(db_conn *db,
                         unique_id object_id,
                         lookup_callback callback);
