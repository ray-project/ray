#ifndef CONFIG_H
#define CONFIG_H

#include <math.h>
#include <stdint.h>

constexpr int64_t RAY_PROTOCOL_VERSION = 0x0000000000000000;

/** The duration between heartbeats. These are sent by the plasma manager and
 *  local scheduler. */
constexpr int64_t HEARTBEAT_TIMEOUT_MILLISECONDS = 100;
/** If a component has not sent a heartbeat in the last NUM_HEARTBEATS_TIMEOUT
 *  heartbeat intervals, the global scheduler or monitor process will report it
 *  as dead to the db_client table. */
constexpr int64_t NUM_HEARTBEATS_TIMEOUT = 100;

/** When performing ray.get, wait 1 second before attemping to reconstruct and
 *  fetch the object again. */
constexpr int64_t GET_TIMEOUT_MILLISECONDS = 1000;

/* Number of times we try binding to a socket. */
constexpr int64_t NUM_BIND_ATTEMPTS = 5;
constexpr int64_t BIND_TIMEOUT_MS = 100;

/* Number of times we try connecting to a socket. */
constexpr int64_t NUM_CONNECT_ATTEMPTS = 50;
constexpr int64_t CONNECT_TIMEOUT_MS = 100;

/* The duration that the local scheduler will wait before reinitiating a fetch
 * request for a missing task dependency. This time may adapt based on the
 * number of missing task dependencies. */
constexpr int64_t kLocalSchedulerFetchTimeoutMilliseconds = 1000;
/* The duration that the local scheduler will wait between initiating
 * reconstruction calls for missing task dependencies. If there are many missing
 * task dependencies, we will only iniate reconstruction calls for some of them
 * each time. */
constexpr int64_t kLocalSchedulerReconstructionTimeoutMilliseconds = 1000;

/* The duration that we wait after sending a worker SIGTERM before sending the
 * worker SIGKILL. */
constexpr int64_t KILL_WORKER_TIMEOUT_MILLISECONDS = 100;

constexpr double kDefaultNumCPUs = INT16_MAX;
constexpr double kDefaultNumGPUs = 0;
constexpr double kDefaultNumCustomResource = INFINITY;

constexpr int64_t MANAGER_TIMEOUT = 1000;
constexpr int64_t BUFSIZE = 4096;

constexpr int64_t max_time_for_handler = 1000;

constexpr int64_t SIZE_LIMIT = 100;
constexpr int64_t NUM_ELEMENTS_LIMIT = 1000;

constexpr int64_t max_time_for_loop = 1000;

/* Allow up to 5 seconds for connecting to Redis. */
constexpr int64_t REDIS_DB_CONNECT_RETRIES = 50;
constexpr int64_t REDIS_DB_CONNECT_WAIT_MS = 100;

constexpr int64_t PLASMA_DEFAULT_RELEASE_DELAY = 64;
constexpr int64_t kL3CacheSizeBytes = 100000000;

#endif /* CONFIG_H */
