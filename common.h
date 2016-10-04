#ifndef COMMON_H
#define COMMON_H

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <errno.h>

#ifndef RAY_COMMON_DEBUG
#define LOG_DEBUG(M, ...)
#else
#define LOG_DEBUG(M, ...) \
  fprintf(stderr, "[DEBUG] (%s:%d) " M "\n", __FILE__, __LINE__, ##__VA_ARGS__)
#endif

#define LOG_ERR(M, ...)                                                     \
  fprintf(stderr, "[ERROR] (%s:%d: errno: %s) " M "\n", __FILE__, __LINE__, \
          errno == 0 ? "None" : strerror(errno), ##__VA_ARGS__)

#define LOG_INFO(M, ...) \
  fprintf(stderr, "[INFO] (%s:%d) " M "\n", __FILE__, __LINE__, ##__VA_ARGS__)

#define CHECK(COND)                        \
  do {                                     \
    if (!(COND)) {                         \
      LOG_ERR("Check failure: %s", #COND); \
      exit(-1);                            \
    }                                      \
  } while (0);

#define CHECKM(COND, M, ...)                                   \
  do {                                                         \
    if (!(COND)) {                                             \
      LOG_ERR("Check failure: %s \n" M, #COND, ##__VA_ARGS__); \
      exit(-1);                                                \
    }                                                          \
  } while (0);

#define UNIQUE_ID_SIZE 20

/* Cleanup method for running tests with the greatest library.
 * Runs the test, then clears the Redis database. */
#define RUN_REDIS_TEST(context, test) \
  RUN_TEST(test);                     \
  freeReplyObject(redisCommand(context, "FLUSHALL"));

typedef struct { unsigned char id[UNIQUE_ID_SIZE]; } unique_id;

extern const unique_id NIL_ID;

/* Generate a globally unique ID. */
unique_id globally_unique_id(void);

/* Convert a 20 byte sha1 hash to a hexdecimal string. This function assumes
 * that buffer points to an already allocated char array of size 2 *
 * UNIQUE_ID_SIZE + 1 */
char *sha1_to_hex(const unsigned char *sha1, char *buffer);

typedef unique_id object_id;

#endif
