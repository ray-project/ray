#include "logging.h"

#include <inttypes.h>
#include <stdint.h>
#include <sys/time.h>

#include <hiredis/hiredis.h>
#include <utstring.h>

#include "state/redis.h"
#include "io.h"

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
