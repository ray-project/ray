#ifndef EVENT_LOOP_H
#define EVENT_LOOP_H

#include <poll.h>
#include <stdint.h>

#include "utarray.h"

typedef struct {
  /* The type of connection (e.g. redis, client, manager, data transfer). */
  int type;
  /* Data associated with the connection (managed by the user) */
  void *data;
} event_loop_item;

typedef struct {
  /* Array of event_loop_items that hold information for connections. */
  UT_array *items;
  /* Array of file descriptors that are waiting, corresponding to items. */
  UT_array *waiting;
} event_loop;

/* Event loop functions. */
void event_loop_init(event_loop *loop);
void event_loop_free(event_loop *loop);
int64_t event_loop_attach(event_loop *loop,
                          int type,
                          void *data,
                          int fd,
                          int events);
void event_loop_detach(event_loop *loop, int64_t index, int shall_close);
int event_loop_poll(event_loop *loop);
int64_t event_loop_size(event_loop *loop);
struct pollfd *event_loop_get(event_loop *loop, int64_t index);
void event_loop_set_data(event_loop *loop, int64_t index, void *data);
void *event_loop_get_data(event_loop *loop, int64_t index);

#endif
