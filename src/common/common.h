#ifndef COMMON_H
#define COMMON_H

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#ifndef __STDC_FORMAT_MACROS
#define __STDC_FORMAT_MACROS
#endif
#include <errno.h>
#include <inttypes.h>
#ifndef _WIN32
#include <execinfo.h>
#endif

#ifdef __cplusplus
#include <functional>
extern "C" {
#endif
#include "sha256.h"
#ifdef __cplusplus
}
#endif

#include "plasma/common.h"
#include "arrow/util/macros.h"

#include "state/ray_config.h"

/** Definitions for Ray logging levels. */
#define RAY_COMMON_DEBUG 0
#define RAY_COMMON_INFO 1
#define RAY_COMMON_WARNING 2
#define RAY_COMMON_ERROR 3
#define RAY_COMMON_FATAL 4

/**
 * RAY_COMMON_LOG_LEVEL should be defined to one of the above logging level
 * integer values. Any logging statement in the code with a logging level
 * greater than or equal to RAY_COMMON_LOG_LEVEL will be outputted to stderr.
 * The default logging level is INFO. */
#ifndef RAY_COMMON_LOG_LEVEL
#define RAY_COMMON_LOG_LEVEL RAY_COMMON_INFO
#endif

/**
 * Macros to enable each level of Ray logging statements depending on the
 * current logging level. */
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
#elif defined(_EXECINFO_H) || !defined(_WIN32)
#define LOG_FATAL(M, ...)                                                     \
  do {                                                                        \
    fprintf(stderr, "[FATAL] (%s:%d: errno: %s) " M "\n", __FILE__, __LINE__, \
            errno == 0 ? "None" : strerror(errno), ##__VA_ARGS__);            \
    void *buffer[255];                                                        \
    const int calls = backtrace(buffer, sizeof(buffer) / sizeof(void *));     \
    backtrace_symbols_fd(buffer, calls, 1);                                   \
    abort();                                                                  \
  } while (0)
#else
#define LOG_FATAL(M, ...)                                                     \
  do {                                                                        \
    fprintf(stderr, "[FATAL] (%s:%d: errno: %s) " M "\n", __FILE__, __LINE__, \
            errno == 0 ? "None" : strerror(errno), ##__VA_ARGS__);            \
    exit(-1);                                                                 \
  } while (0)
#endif

/** Assertion definitions, with optional logging. */
#define CHECKM(COND, M, ...)                                   \
  if (!(COND)) {                                               \
    LOG_FATAL("Check failure: %s \n" M, #COND, ##__VA_ARGS__); \
  }

#define CHECK(COND) CHECKM(COND, "")

#define RAY_DCHECK(COND) CHECK(COND)

/* These are exit codes for common errors that can occur in Ray components. */
#define EXIT_COULD_NOT_BIND_PORT -2

/** This macro indicates that this pointer owns the data it is pointing to
 *  and is responsible for freeing it. */
#define OWNER

/** Definitions for unique ID types. */
#define UNIQUE_ID_SIZE 20

#define UNIQUE_ID_EQ(id1, id2) (memcmp((id1).id, (id2).id, UNIQUE_ID_SIZE) == 0)

#define IS_NIL_ID(id) UNIQUE_ID_EQ(id, NIL_ID)

struct UniqueID {
  unsigned char id[UNIQUE_ID_SIZE];
  UniqueID(const plasma::UniqueID &from) {
    memcpy(&id[0], from.data(), UNIQUE_ID_SIZE);
  }
  UniqueID() {}
  static const UniqueID nil() {
    UniqueID result;
    std::fill_n(result.id, UNIQUE_ID_SIZE, 255);
    return result;
  }
  plasma::UniqueID to_plasma_id() {
    plasma::UniqueID result;
    memcpy(result.mutable_data(), &id[0], UNIQUE_ID_SIZE);
    return result;
  }
};

extern const UniqueID NIL_ID;

/* Generate a globally unique ID. */
UniqueID globally_unique_id(void);

#define NIL_OBJECT_ID NIL_ID
#define NIL_WORKER_ID NIL_ID

/** The object ID is the type used to identify objects. */
typedef UniqueID ObjectID;

#ifdef __cplusplus

struct UniqueIDHasher {
  /* ObjectID hashing function. */
  size_t operator()(const UniqueID &id) const {
    size_t result;
    memcpy(&result, id.id, sizeof(size_t));
    return result;
  }
};

bool operator==(const ObjectID &x, const ObjectID &y);
#endif

#define ID_STRING_SIZE (2 * UNIQUE_ID_SIZE + 1)

/**
 * Convert an object ID to a hexdecimal string. This function assumes that
 * buffer points to an already allocated char array of size ID_STRING_SIZE. And
 * it writes a null-terminated hex-formatted string to id_string.
 *
 * @param obj_id The object ID to convert to a string.
 * @param id_string A buffer to write the string to. It is assumed that this is
 *        managed by the caller and is sufficiently long to store the object ID
 *        string.
 * @param id_length The length of the id_string buffer.
 */
char *ObjectID_to_string(ObjectID obj_id, char *id_string, int id_length);

/**
 * Compare two object IDs.
 *
 * @param first_id The first object ID to compare.
 * @param second_id The first object ID to compare.
 * @return True if the object IDs are the same and false otherwise.
 */
bool ObjectID_equal(ObjectID first_id, ObjectID second_id);

/**
 * Compare a object ID to the nil ID.
 *
 * @param id The object ID to compare to nil.
 * @return True if the object ID is equal to nil.
 */
bool ObjectID_is_nil(ObjectID id);

/** The worker ID is the ID of a worker or driver. */
typedef UniqueID WorkerID;

/**
 * Compare two worker IDs.
 *
 * @param first_id The first worker ID to compare.
 * @param second_id The first worker ID to compare.
 * @return True if the worker IDs are the same and false otherwise.
 */
bool WorkerID_equal(WorkerID first_id, WorkerID second_id);

typedef UniqueID DBClientID;

/**
 * Compare two db client IDs.
 *
 * @param first_id The first db client ID to compare.
 * @param second_id The first db client ID to compare.
 * @return True if the db client IDs are the same and false otherwise.
 */
bool DBClientID_equal(DBClientID first_id, DBClientID second_id);

/**
 * Compare a db client ID to the nil ID.
 *
 * @param id The db client ID to compare to nil.
 * @return True if the db client ID is equal to nil.
 */
bool DBClientID_is_nil(ObjectID id);

#define MAX(x, y) ((x) >= (y) ? (x) : (y))
#define MIN(x, y) ((x) <= (y) ? (x) : (y))

/** Definitions for computing hash digests. */
#define DIGEST_SIZE SHA256_BLOCK_SIZE

extern const unsigned char NIL_DIGEST[DIGEST_SIZE];

/**
 * Return the current time in milliseconds since the Unix epoch.
 *
 * @return The number of milliseconds since the Unix epoch.
 */
int64_t current_time_ms();

#endif
