#include "event_loop.h"

#include "common.h"
#include <errno.h>

#define INITIAL_EVENT_LOOP_SIZE 1024

event_loop *event_loop_create(void) {
  return aeCreateEventLoop(INITIAL_EVENT_LOOP_SIZE);
}

void event_loop_destroy(event_loop *loop) {
  /* Clean up timer events. This is to make valgrind happy. */
  aeTimeEvent *te = loop->timeEventHead;
  while (te) {
    aeTimeEvent *next = te->next;
    free(te);
    te = next;
  }
  aeDeleteEventLoop(loop);
}

void event_loop_add_file(event_loop *loop,
                         int fd,
                         int events,
                         event_loop_file_handler handler,
                         void *context) {
  /* Try to add the file descriptor. */
  int err = aeCreateFileEvent(loop, fd, events, handler, context);
  /* If it cannot be added, increase the size of the event loop. */
  if (err == AE_ERR && errno == ERANGE) {
    err = aeResizeSetSize(loop, 3 * aeGetSetSize(loop) / 2);
    CHECK(err == AE_OK);
    err = aeCreateFileEvent(loop, fd, events, handler, context);
  }
  /* In any case, test if there were errors. */
  CHECK(err == AE_OK);
}

void event_loop_remove_file(event_loop *loop, int fd) {
  aeDeleteFileEvent(loop, fd, EVENT_LOOP_READ | EVENT_LOOP_WRITE);
}

int64_t event_loop_add_timer(event_loop *loop,
                             int64_t milliseconds,
                             event_loop_timer_handler handler,
                             void *context) {
  return aeCreateTimeEvent(loop, milliseconds, handler, context, NULL);
}

void event_loop_remove_timer(event_loop *loop, timer_id timer_id) {
  int err = aeDeleteTimeEvent(loop, timer_id);
  CHECK(err == AE_OK); /* timer id found? */
}

void event_loop_run(event_loop *loop) {
  aeMain(loop);
}

void event_loop_stop(event_loop *loop) {
  aeStop(loop);
}
