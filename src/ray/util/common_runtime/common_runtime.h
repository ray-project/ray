/**
 * This file defines the API interface for a common local runtime
 * that is useful for building most distributed systems.
 * 
 * The implementation of the API is in the %providers% directory
 * within which multiple implementations are provided and we can choose
 * one of them during build (or CMake).
 * 
 * Goal of this interface:
 *   - meet the common requirements for building most distributed systems
 *   - general enough for possibility of plugging in new implmenetations (e.g., providers)
 *   - the generalization should not introduce runtime overhead
 *      * performance wise, we should use macros for that
 *      * but, simple forwarded call should be able to be inlined by compilers, so here
 *        we still use function wrappers instead
 */
#ifndef RAY_UTIL_COMMON_RUNTIME_H
#define RAY_UTIL_COMMON_RUNTIME_H

// logging
# include "interface/logging.h"

// configuration
# include "interface/configuration.h"

// TODO: performance counters

// TODO: rpc

// TODO: event loop (including timers)

// TODO: disk io

#endif  // RAY_UTIL_COMMON_RUNTIME_H