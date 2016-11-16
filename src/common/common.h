#ifndef COMMON_H
#define COMMON_H

#include <stdbool.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <errno.h>
#include <inttypes.h>
#include <execinfo.h>

#include "utarray.h"

#define RAY_COMMON_DEBUG 0
#define RAY_COMMON_INFO 1
#define RAY_COMMON_WARNING 2
#define RAY_COMMON_ERROR 3
#define RAY_COMMON_FATAL 4

/* Default logging level is INFO. */
#ifndef RAY_COMMON_LOG_LEVEL
#define RAY_COMMON_LOG_LEVEL RAY_COMMON_INFO
#endif

#if (RAY_COMMON_LOG_LEVEL > RAY_COMMON_DEBUG)
#define LOG_DEBUG(M, ...)
#else
#define LOG_DEBUG(M, ...) \
  fprintf(stderr, "[DEBUG] (%s:%d) " M "\n", __FILE__, __LINE__, ##__VA_ARGS__)
#endif

#if (RAY_COMMON_LOG_LEVEL > RAY_COMMON_INFO)
#define LOG_INFO(M, ...)
#else
#define LOG_INFO(M, ...) \
  fprintf(stderr, "[INFO] (%s:%d) " M "\n", __FILE__, __LINE__, ##__VA_ARGS__)
#endif

#if (RAY_COMMON_LOG_LEVEL > RAY_COMMON_WARNING)
#define LOG_WARN(M, ...)
#else
#define LOG_WARN(M, ...) \
  fprintf(stderr, "[WARN] (%s:%d) " M "\n", __FILE__, __LINE__, ##__VA_ARGS__)
#endif

#if (RAY_COMMON_LOG_LEVEL > RAY_COMMON_ERROR)
#define LOG_ERROR(M, ...)
#else
#define LOG_ERROR(M, ...)                                                   \
  fprintf(stderr, "[ERROR] (%s:%d: errno: %s) " M "\n", __FILE__, __LINE__, \
          errno == 0 ? "None" : strerror(errno), ##__VA_ARGS__)
#endif

#if (RAY_COMMON_LOG_LEVEL > RAY_COMMON_FATAL)
#define LOG_FATAL(M, ...)
#else
#define LOG_FATAL(M, ...)                                                 \
  do {                                                                    \
    fprintf(stderr, "[FATAL] (%s:%d) " M "\n", __FILE__, __LINE__,        \
            ##__VA_ARGS__);                                               \
    void *buffer[255];                                                    \
    const int calls = backtrace(buffer, sizeof(buffer) / sizeof(void *)); \
    backtrace_symbols_fd(buffer, calls, 1);                               \
    exit(-1);                                                             \
  } while (0);
#endif

#define CHECKM(COND, M, ...)                                   \
  if (!(COND)) {                                               \
    LOG_FATAL("Check failure: %s \n" M, #COND, ##__VA_ARGS__); \
  }

#define CHECK(COND) CHECKM(COND, "")

/* This should be defined if we want to check calls to DCHECK. */
#define RAY_DCHECK

#ifdef RAY_DCHECK
#define DCHECK(COND) CHECK(COND)
#else
#define DCHECK(COND)
#endif

/* These are exit codes for common errors that can occur in Ray components. */
#define EXIT_COULD_NOT_BIND_PORT -2

/** This macro indicates that this pointer owns the data it is pointing to
 *  and is responsible for freeing it. */
#define OWNER

#define UNIQUE_ID_SIZE 20

#define UNIQUE_ID_EQ(id1, id2) (memcmp((id1).id, (id2).id, UNIQUE_ID_SIZE) == 0)

#define IS_NIL_ID(id) UNIQUE_ID_EQ(id, NIL_ID)

typedef struct { unsigned char id[UNIQUE_ID_SIZE]; } unique_id;

extern const UT_icd object_id_icd;

extern const unique_id NIL_ID;

/* Generate a globally unique ID. */
unique_id globally_unique_id(void);

/* Convert a 20 byte sha1 hash to a hexdecimal string. This function assumes
 * that buffer points to an already allocated char array of size 2 *
 * UNIQUE_ID_SIZE + 1 */
char *sha1_to_hex(const unsigned char *sha1, char *buffer);

#define NIL_OBJECT_ID NIL_ID

typedef unique_id object_id;

/**
 * Compare two object IDs.
 *
 * @param first_id The first object ID to compare.
 * @param second_id The first object ID to compare.
 * @return True if the object IDs are the same and false otherwise.
 */
bool object_ids_equal(object_id first_id, object_id second_id);

/**
 * Compare a object ID to the nil ID.
 *
 * @param id The object ID to compare to nil.
 * @return True if the object ID is equal to nil.
 */
bool object_id_is_nil(object_id id);

#endif
