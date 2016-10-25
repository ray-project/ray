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

typedef struct ray_logger_impl ray_logger;

/* Initialize a Ray logger for the given client type and logging level. If the
 * is_direct flag is set, the logger will treat the given connection as a
 * direct connection to the log. Otherwise, it will treat it as a socket to
 * another process with a connection to the log.
 * NOTE: User is responsible for freeing the returned logger. */
ray_logger *init_ray_logger(const char *client_type,
                            int log_level,
                            int is_direct,
                            void *conn);

/* Free the logger. This does not free the connection to the log. */
void free_ray_logger(ray_logger *logger);

/* Log an event at the given log level with the given event_type.
 * NOTE: message cannot contain spaces! JSON format is recommended.
 * TODO: Support spaces in messages. */
void ray_log(ray_logger *logger,
             int log_level,
             const char *event_type,
             const char *message);

#endif
