#include "logging.h"

#include <inttypes.h>
#include <stdint.h>
#include <sys/time.h>

#include <hiredis/hiredis.h>

#include "state/redis.h"
#include "io.h"
#include <iostream>
#include <string>

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
  if (log_level < RAY_LOG_DEBUG || log_level > RAY_LOG_FATAL) {
    return;
  }
  struct timeval tv;
  gettimeofday(&tv, NULL);
  std::string timestamp =
      std::to_string(tv.tv_sec) + "." + std::to_string(tv.tv_usec);

  /* Find number of bytes that would have been written for formatted_message
   * size */
  size_t formatted_message_size =
      std::snprintf(nullptr, 0, log_fmt, timestamp.c_str(), "%b",
                    log_levels[log_level], event_type, message,
                    timestamp.c_str()) +
      1;
  /* Fill out everything except the client ID, which is binary data. */
  char formatted_message[formatted_message_size];
  std::snprintf(formatted_message, formatted_message_size, log_fmt,
                timestamp.c_str(), "%b", log_levels[log_level], event_type,
                message, timestamp.c_str());

  if (logger->is_direct) {
    DBHandle *db = (DBHandle *) logger->conn;
    /* Fill in the client ID and send the message to Redis. */

    redisAsyncContext *context = get_redis_context(db, db->client);

    int status =
        redisAsyncCommand(context, NULL, NULL, formatted_message,
                          (char *) db->client.data(), sizeof(db->client));
    if ((status == REDIS_ERR) || context->err) {
      LOG_REDIS_DEBUG(context, "error while logging message to log table");
    }
  } else {
    /* If we don't own a Redis connection, we leave our client
     * ID to be filled in by someone else. */
    int *socket_fd = (int *) logger->conn;
    write_log_message(*socket_fd, formatted_message);
  }
}

void RayLogger_log_event(DBHandle *db,
                         uint8_t *key,
                         int64_t key_length,
                         uint8_t *value,
                         int64_t value_length,
                         double timestamp) {
  std::string timestamp_string = std::to_string(timestamp);
  int status = redisAsyncCommand(db->context, NULL, NULL, "ZADD %b %s %b", key,
                                 key_length, timestamp_string.c_str(), value,
                                 value_length);
  if ((status == REDIS_ERR) || db->context->err) {
    LOG_REDIS_DEBUG(db->context, "error while logging message to event log");
  }
}
