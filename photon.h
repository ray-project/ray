#ifndef PHOTON_H
#define PHOTON_H

enum photon_message_type {
  /** Notify the local scheduler that a task has finished. */
  TASK_DONE = 64,
};

struct photon_conn_impl {
  /* File descriptor of the Unix domain socket that connects to photon. */
  int conn;
};

#endif
