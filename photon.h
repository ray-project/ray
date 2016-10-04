#ifndef PHOTON_H
#define PHOTON_H

enum photon_message_type {
  /** Notify the local scheduler that a task has finished. */
  TASK_DONE = 64,
  /** Get a new task from the local scheduler. */
  GET_TASK,
  /** This is sent from the local scheduler to a worker to tell the worker to
   *  execute a task. */
  EXECUTE_TASK,
};

#endif
