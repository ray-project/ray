#ifndef EVENT_LOOP_H
#define EVENT_LOOP_H

#include <poll.h>

#include "utarray.h"
#include "plasma.h"
#include "plasma_manager.h"

typedef struct {
  /* The type of connection (e.g. redis, client, manager, data transfer). */
  int type;
  /* If type is data transfer, this contains information about the status
   * of the transfer. */
	data_connection connection;
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
int64_t event_loop_attach(event_loop *loop, int type, data_connection* connection, int fd, int events);
void event_loop_detach(event_loop *loop, int64_t index, int shall_close);
int event_loop_poll(event_loop *loop);
int64_t event_loop_size(event_loop *loop);
struct pollfd *event_loop_get(event_loop *loop, int64_t index);
void event_loop_set_connection(event_loop *loop, int64_t index, const data_connection* conn);
data_connection *event_loop_get_connection(event_loop *loop, int64_t index);

#endif
