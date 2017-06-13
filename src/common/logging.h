#ifndef LOGGING_H
#define LOGGING_H

#define RAY_VERBOSE -1
#define RAY_DEBUG 0
#define RAY_INFO 1
#define RAY_WARNING 2
#define RAY_ERROR 3
#define RAY_FATAL 4

/* Entity types. */
#define RAY_FUNCTION "FUNCTION"
#define RAY_OBJECT "OBJECT"
#define RAY_TASK "TASK"

#include "state/db.h"

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
                         int64_t value_length);

#endif /* LOGGING_H */
