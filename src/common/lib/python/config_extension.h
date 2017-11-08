#ifndef CONFIG_EXTENSION_H
#define CONFIG_EXTENSION_H

#include <Python.h>

#include "common.h"

// clang-format off
typedef struct {
  PyObject_HEAD
} PyRayConfig;
// clang-format on

extern PyTypeObject PyRayConfigType;

/* Create a PyRayConfig from C++. */
PyObject *PyRayConfig_make();

PyObject *PyRayConfig_kRayProtocolVersion(PyObject *self);
PyObject *PyRayConfig_kHeartbeatTimeoutMilliseconds(PyObject *self);
PyObject *PyRayConfig_kNumHeartbeatsTimeout(PyObject *self);
PyObject *PyRayConfig_kGetTimeoutMilliseconds(PyObject *self);
PyObject *PyRayConfig_kNumBindAttempts(PyObject *self);
PyObject *PyRayConfig_kBindTimeoutMilliseconds(PyObject *self);
PyObject *PyRayConfig_kNumConnectAttempts(PyObject *self);
PyObject *PyRayConfig_kConnectTimeoutMilliseconds(PyObject *self);
PyObject *PyRayConfig_kLocalSchedulerFetchTimeoutMilliseconds(PyObject *self);
PyObject *PyRayConfig_kLocalSchedulerReconstructionTimeoutMilliseconds(
    PyObject *self);
PyObject *PyRayConfig_kKillWorkerTimeoutMilliseconds(PyObject *self);
PyObject *PyRayConfig_kDefaultNumCPUs(PyObject *self);
PyObject *PyRayConfig_kDefaultNumGPUs(PyObject *self);
PyObject *PyRayConfig_kDefaultNumCustomResource(PyObject *self);
PyObject *PyRayConfig_kManagerTimeoutMilliseconds(PyObject *self);
PyObject *PyRayConfig_kBufSize(PyObject *self);
PyObject *PyRayConfig_kMaxTimeForHandlerMilliseconds(PyObject *self);
PyObject *PyRayConfig_kSizeLimit(PyObject *self);
PyObject *PyRayConfig_kNumElementsLimit(PyObject *self);
PyObject *PyRayConfig_kMaxTimeForLoop(PyObject *self);
PyObject *PyRayConfig_kRedisDBConnectRetries(PyObject *self);
PyObject *PyRayConfig_kRedisDBConnectWaitMilliseconds(PyObject *self);
PyObject *PyRayConfig_kPlasmaDefaultReleaseDelay(PyObject *self);
PyObject *PyRayConfig_kL3CacheSizeBytes(PyObject *self);

#endif /* CONFIG_EXTENSION_H */
