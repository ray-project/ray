#ifndef PHOTON_H
#define PHOTON_H

#include "common/task.h"
#include "common/state/db.h"
#include "utarray.h"
#include "uthash.h"

enum photon_message_type {
  /** Notify the local scheduler that a task has finished. */
  TASK_DONE = 64,
  /** Get a new task from the local scheduler. */
  GET_TASK,
  /** This is sent from the local scheduler to a worker to tell the worker to
   *  execute a task. */
  EXECUTE_TASK,
};

// clang-format off
/** Contains all information that is associated to a worker. */
typedef struct {
  int sock;
} worker;
// clang-format on

/* These are needed to define the UT_arrays. */
UT_icd task_ptr_icd;
UT_icd worker_icd;

/** Resources that are exposed to the scheduling algorithm. */
typedef struct {
  /** List of workers available to this node. The index into this array
   *  is the worker_index and is used to identify workers throughout
   *  the program. */
  UT_array *workers;
  /* The handle to the database. */
  db_handle *db;
} scheduler_info;

#endif /* PHOTON_H */
