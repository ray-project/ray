#ifndef PHOTON_SCHEDULER
#define PHOTON_SCHEDULER

/* Establish a connection to a new client. */
void new_client_connection(local_scheduler_state *s, int listener_sock);

/* schedule a task on a given worker. */
void schedule_on_worker(local_scheduler_state *s, task_spec *task,
                        int client_id);

/* Handle new incoming task that was scheduled by the globl scheduler on
 * this local scheduler. */
void schedule_task(local_scheduler_state *s, task_spec *task)

#endif
