#ifndef LOGGING_H
#define LOGGING_H

#define RAY_LOG_VERBOSE -1
#define RAY_LOG_DEBUG 0
#define RAY_LOG_INFO 1
#define RAY_LOG_WARNING 2
#define RAY_LOG_ERROR 3
#define RAY_LOG_FATAL 4

/* Entity types. */
#define RAY_FUNCTION "FUNCTION"
#define RAY_OBJECT "OBJECT"
#define RAY_TASK "TASK"

#include "state/db.h"

typedef struct RayLoggerImpl RayLogger;

/* Initialize a Ray logger for the given client type and logging level. If the
 * is_direct flag is set, the logger will treat the given connection as a
 * direct connection to the log. Otherwise, it will treat it as a socket to
 * another process with a connection to the log.
 * NOTE: User is responsible for freeing the returned logger. */
RayLogger *RayLogger_init(const char *client_type,
                          int log_level,
                          int is_direct,
                          void *conn);

/* Free the logger. This does not free the connection to the log. */
void RayLogger_free(RayLogger *logger);

/* Log an event at the given log level with the given event_type.
 * NOTE: message cannot contain spaces! JSON format is recommended.
 * TODO: Support spaces in messages. */
void RayLogger_log(RayLogger *logger,
                   int log_level,
                   const char *event_type,
                   const char *message);

/**
 * Log an event to the event log.
 *
 * @param db The database handle.
 * @param key The key in Redis to store the event in.
 * @param key_length The length of the key.
 * @param value The value to log.
 * @param value_length The length of the value.
 * @return Void.
 */
void RayLogger_log_event(DBHandle *db,
                         uint8_t *key,
                         int64_t key_length,
                         uint8_t *value,
                         int64_t value_length,
                         double time);

#endif /* LOGGING_H */
