#include "logging.h"

#include <inttypes.h>
#include <stdint.h>
#include <sys/time.h>

#include <hiredis/hiredis.h>
#include <utstring.h>

#include "state/redis.h"
#include "io.h"

static const char *log_levels[5] = {"DEBUG", "INFO", "WARN", "ERROR", "FATAL"};
static const char *log_fmt =
    "HMSET log:%s:%s log_level %s event_type %s message %s timestamp %s";

struct RayLoggerImpl {
  /* String that identifies this client type. */
  const char *client_type;
  /* Suppress all log messages below this level. */
  int log_level;
  /* Whether or not we have a direct connection to Redis. */
  int is_direct;
  /* Either a db_handle or a socket to a process with a db_handle,
   * depending on the is_direct flag. */
  void *conn;
};

RayLogger *RayLogger_init(const char *client_type,
                          int log_level,
                          int is_direct,
                          void *conn) {
  RayLogger *logger = (RayLogger *) malloc(sizeof(RayLogger));
  logger->client_type = client_type;
  logger->log_level = log_level;
  logger->is_direct = is_direct;
  logger->conn = conn;
  return logger;
}

void RayLogger_free(RayLogger *logger) {
  free(logger);
}

void RayLogger_log(RayLogger *logger,
                   int log_level,
                   const char *event_type,
                   const char *message) {
  if (log_level < logger->log_level) {
    return;
  }
  if (log_level < RAY_DEBUG || log_level > RAY_FATAL) {
    return;
  }
  struct timeval tv;
  UT_string *timestamp;
  utstring_new(timestamp);
  gettimeofday(&tv, NULL);
  utstring_printf(timestamp, "%ld.%ld", tv.tv_sec, (long) tv.tv_usec);

  UT_string *formatted_message;
  utstring_new(formatted_message);
  /* Fill out everything except the client ID, which is binary data. */
  utstring_printf(formatted_message, log_fmt, utstring_body(timestamp), "%b",
                  log_levels[log_level], event_type, message,
                  utstring_body(timestamp));
  if (logger->is_direct) {
    DBHandle *db = (DBHandle *) logger->conn;
    /* Fill in the client ID and send the message to Redis. */

    redisAsyncContext *context = get_redis_context(db, db->client);

    int status = redisAsyncCommand(
        context, NULL, NULL, utstring_body(formatted_message),
        (char *) db->client.id, sizeof(db->client.id));
    if ((status == REDIS_ERR) || context->err) {
      LOG_REDIS_DEBUG(context, "error while logging message to log table");
    }
  } else {
    /* If we don't own a Redis connection, we leave our client
     * ID to be filled in by someone else. */
    int *socket_fd = (int *) logger->conn;
    write_log_message(*socket_fd, utstring_body(formatted_message));
  }
  utstring_free(formatted_message);
  utstring_free(timestamp);
}

void RayLogger_log_event(DBHandle *db,
                         uint8_t *key,
                         int64_t key_length,
                         uint8_t *value,
                         int64_t value_length) {
  int status = redisAsyncCommand(db->context, NULL, NULL, "RPUSH %b %b", key,
                                 key_length, value, value_length);
  if ((status == REDIS_ERR) || db->context->err) {
    LOG_REDIS_DEBUG(db->context, "error while logging message to event log");
  }
}
