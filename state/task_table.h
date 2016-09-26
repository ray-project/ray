#ifndef TASK_TABLE_H
#define TASK_TABLE_H

#include "db.h"
#include "task.h"

/* Add task to the task table, handle errors here. */
status task_table_add_task(db_handle *db, task_iid task_iid, task_spec *task);

/* Get specific task from the task table. */
status task_table_get_task(db_handle *db, task_iid task_iid, task_spec *task);

#endif /* TASK_TABLE_H */
