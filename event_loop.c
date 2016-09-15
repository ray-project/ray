#include "event_loop.h"

#include <assert.h>

UT_icd item_icd = {sizeof(event_loop_item), NULL, NULL, NULL};
UT_icd poll_icd = {sizeof(struct pollfd), NULL, NULL, NULL};

/* Initializes the event loop.
 * This function needs to be called before any other event loop function. */
void event_loop_init(event_loop *loop) {
  utarray_new(loop->items, &item_icd);
  utarray_new(loop->waiting, &poll_icd);
}

/* Add a new file descriptor fd to the event loop.
 * This function sets a user defined type and id for the file descriptor
 * which can be queried using event_loop_type and event_loop_id. The parameter
 * events is the same as in http://linux.die.net/man/2/poll.
 * Returns the index of the item in the event loop. */
int64_t event_loop_attach(event_loop *loop,
                          int type,
                          void *data,
                          int fd,
                          int events) {
  assert(utarray_len(loop->items) == utarray_len(loop->waiting));
  int64_t index = utarray_len(loop->items);
  event_loop_item item = {.type = type, .data = data};
  utarray_push_back(loop->items, &item);
  struct pollfd waiting = {.fd = fd, .events = events};
  utarray_push_back(loop->waiting, &waiting);
  return index;
}

/* Detach a file descriptor from the event loop.
 * This invalidates all other indices into the event loop items, but leaves
 * the ids of the event loop items valid. */
void event_loop_detach(event_loop *loop, int64_t index, int shall_close) {
  struct pollfd *waiting_item =
      (struct pollfd *) utarray_eltptr(loop->waiting, index);
  struct pollfd *waiting_back = (struct pollfd *) utarray_back(loop->waiting);
  if (shall_close) {
    close(waiting_item->fd);
  }
  *waiting_item = *waiting_back;
  utarray_pop_back(loop->waiting);

  event_loop_item *items_item =
      (event_loop_item *) utarray_eltptr(loop->items, index);
  event_loop_item *items_back = (event_loop_item *) utarray_back(loop->items);
  *items_item = *items_back;
  utarray_pop_back(loop->items);
}

/* Poll the file descriptors associated to this event loop.
 * See http://linux.die.net/man/2/poll */
int event_loop_poll(event_loop *loop) {
  return poll((struct pollfd *) utarray_front(loop->waiting),
              utarray_len(loop->waiting), -1);
}

/* Get the total number of file descriptors participating in the event loop. */
int64_t event_loop_size(event_loop *loop) {
  return utarray_len(loop->waiting);
}

/* Get the pollfd structure associated to a file descriptor participating in the
 * event loop. */
struct pollfd *event_loop_get(event_loop *loop, int64_t index) {
  return (struct pollfd *) utarray_eltptr(loop->waiting, index);
}

/* Set the data connection information for participant in the event loop. */
void event_loop_set_data(event_loop *loop, int64_t index, void *data) {
  event_loop_item *item =
      (event_loop_item *) utarray_eltptr(loop->items, index);
  item->data = data;
}

/* Get the data connection information for participant in the event loop. */
void *event_loop_get_data(event_loop *loop, int64_t index) {
  event_loop_item *item =
      (event_loop_item *) utarray_eltptr(loop->items, index);
  return item->data;
}

/* Free the space associated to the event loop.
 * Does not free the event_loop datastructure itself. */
void event_loop_free(event_loop *loop) {
  utarray_free(loop->items);
  utarray_free(loop->waiting);
}
