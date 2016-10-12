#ifndef EVENT_LOOP_H
#define EVENT_LOOP_H

#include <stdint.h>
#include "ae/ae.h"

typedef long long timer_id;

typedef aeEventLoop event_loop;

/* File descriptor is readable. */
#define EVENT_LOOP_READ AE_READABLE

/* File descriptor is writable. */
#define EVENT_LOOP_WRITE AE_WRITABLE

/* Constant specifying that the timer is done and it will be removed. */
#define EVENT_LOOP_TIMER_DONE AE_NOMORE

/* Signature of the handler that will be called when there is a new event
 * on the file descriptor that this handler has been registered for. The
 * context is the one that was passed into add_file by the user. The
 * events parameter indicates which event is available on the file,
 * it can be EVENT_LOOP_READ or EVENT_LOOP_WRITE. */
typedef void (*event_loop_file_handler)(event_loop *loop,
                                        int fd,
                                        void *context,
                                        int events);

/* This handler will be called when a timer times out. The id of the timer
 * as well as the context that was specified when registering this handler
 * are passed as arguments. The return is the number of milliseconds the
 * timer shall be reset to or EVENT_LOOP_TIMER_DONE if the timer shall
 * not be triggered again. */
typedef int (*event_loop_timer_handler)(event_loop *loop,
                                        timer_id timer_id,
                                        void *context);

/* Create and return a new event loop. */
event_loop *event_loop_create();

/* Deallocate space associated with the event loop that was created
 * with the "create" function. */
void event_loop_destroy(event_loop *loop);

/* Register a handler that will be called any time a new event happens on
 * a file descriptor. Can specify a context that will be passed as an
 * argument to the handler. Currently there can only be one handler per file.
 * The events parameter specifies which events we listen to: EVENT_LOOP_READ
 * or EVENT_LOOP_WRITE. */
void event_loop_add_file(event_loop *loop,
                         int fd,
                         int events,
                         event_loop_file_handler handler,
                         void *context);

/* Remove a registered file event handler from the event loop. */
void event_loop_remove_file(event_loop *loop, int fd);

/* Register a handler that will be called after a time slice of
 * "milliseconds" milliseconds. Can specify a context that will be passed
 * as an argument to the handler. Return the id of the time event. */
int64_t event_loop_add_timer(event_loop *loop,
                             int64_t milliseconds,
                             event_loop_timer_handler handler,
                             void *context);

/* Remove a registered time event handler from the event loop. */
void event_loop_remove_timer(event_loop *loop, timer_id timer_id);

/* Run the event loop. */
void event_loop_run(event_loop *loop);

/* Stop the event loop. */
void event_loop_stop(event_loop *loop);

#endif
