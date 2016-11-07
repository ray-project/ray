#ifndef TASK_TABLE_H
#define TASK_TABLE_H

#include "db.h"
#include "table.h"
#include "task.h"

/* Callback for getting and adding an entry from the task
 * table.  Task spec will be freed by the system after the
 * callback */
typedef void (*task_table_callback)(task_id task_id,
                                    task_spec *task,
                                    void *context);

/** Add task to the task table, handle errors here. */
void task_table_add_task(db_handle *db,
                         task_id task_id,
                         task_spec *task,
                         retry_info *retry,
                         task_table_callback done_callback,
                         void *user_context);

/* Get specific task from the task table. */
void task_table_get_task(db_handle *db,
                         task_id task_id,
                         retry_info *retry,
                         task_table_callback done_callback,
                         void *user_context);

#endif /* TASK_TABLE_H */
