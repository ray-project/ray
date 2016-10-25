#ifndef TASK_TABLE_H
#define TASK_TABLE_H

#include "db.h"
#include "task.h"

/* Add task to the task table, handle errors here. */
status task_table_add_task(db_handle *db, task_spec *task);

/* Callback for getting an entry from the task table. Task spec will be freed
 * by the system after the callback */
typedef void (*task_table_callback)(task_spec *task, void *context);

/* Get specific task from the task table. */
status task_table_get_task(db_handle *db,
                           task_id task_id,
                           task_table_callback callback,
                           void *context);

#endif /* TASK_TABLE_H */
