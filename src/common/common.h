#ifndef COMMON_H
#define COMMON_H

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <errno.h>
#include <inttypes.h>
#include <execinfo.h>

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

#define CHECKM(COND, M, ...)                                                \
  do {                                                                      \
    if (!(COND)) {                                                          \
      LOG_ERR("Check failure: %s \n" M, #COND, ##__VA_ARGS__);              \
      void *buffer[255];                                                    \
      const int calls = backtrace(buffer, sizeof(buffer) / sizeof(void *)); \
      backtrace_symbols_fd(buffer, calls, 1);                               \
      exit(-1);                                                             \
    }                                                                       \
  } while (0);

#define CHECK(COND) CHECKM(COND, "")

/* These are exit codes for common errors that can occur in Ray components. */
#define EXIT_COULD_NOT_BIND_PORT -2

/** This macro indicates that this pointer owns the data it is pointing to
 *  and is responsible for freeing it. */
#define OWNER

#define UNIQUE_ID_SIZE 20

typedef struct { unsigned char id[UNIQUE_ID_SIZE]; } unique_id;

extern const unique_id NIL_ID;

/* Generate a globally unique ID. */
unique_id globally_unique_id(void);

/* Convert a 20 byte sha1 hash to a hexdecimal string. This function assumes
 * that buffer points to an already allocated char array of size 2 *
 * UNIQUE_ID_SIZE + 1 */
char *sha1_to_hex(const unsigned char *sha1, char *buffer);

typedef unique_id object_id;

/* Compare two IDs for equaity. */
#define CMP_ID(x, y) (memcmp((void *)(x), (void*)(y), UNIQUE_ID_SIZE) == 0 ? true : false)

#define MAX(x, y) ((x) >= (y) ? (x) : (y))
#define MIN(x, y) ((x) <= (y) ? (x) : (y))

#endif
