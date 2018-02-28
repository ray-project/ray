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

#include "arrow/util/macros.h"
#include "plasma/common.h"
#include "ray/id.h"
#include "ray/util/logging.h"

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

/* These are exit codes for common errors that can occur in Ray components. */
#define EXIT_COULD_NOT_BIND_PORT -2

/** This macro indicates that this pointer owns the data it is pointing to
 *  and is responsible for freeing it. */
#define OWNER

/** The worker ID is the ID of a worker or driver. */
typedef ray::UniqueID WorkerID;

typedef ray::UniqueID DBClientID;

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
