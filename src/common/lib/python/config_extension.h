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
PyObject *PyRayConfig_kLocalSchedulerReconstructionTimeoutMilliseconds(PyObject *self);
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

static PyMethodDef PyRayConfig_methods[] = {
    {"kRayProtocolVersion", (PyCFunction) PyRayConfig_kRayProtocolVersion, METH_NOARGS, "Return kRayProtocolVersion"},
    {"kHeartbeatTimeoutMilliseconds", (PyCFunction) PyRayConfig_kHeartbeatTimeoutMilliseconds, METH_NOARGS, "Return kHeartbeatTimeoutMilliseconds"},
    {"kNumHeartbeatsTimeout", (PyCFunction) PyRayConfig_kNumHeartbeatsTimeout, METH_NOARGS, "Return kNumHeartbeatsTimeout"},
    {"kGetTimeoutMilliseconds", (PyCFunction) PyRayConfig_kGetTimeoutMilliseconds, METH_NOARGS, "Return kGetTimeoutMilliseconds"},
    {"kNumBindAttempts", (PyCFunction) PyRayConfig_kNumBindAttempts, METH_NOARGS, "Return kNumBindAttempts"},
    {"kBindTimeoutMilliseconds", (PyCFunction) PyRayConfig_kBindTimeoutMilliseconds, METH_NOARGS, "Return kBindTimeoutMilliseconds"},
    {"kNumConnectAttempts", (PyCFunction) PyRayConfig_kNumConnectAttempts, METH_NOARGS, "Return kNumConnectAttempts"},
    {"kConnectTimeoutMilliseconds", (PyCFunction) PyRayConfig_kConnectTimeoutMilliseconds, METH_NOARGS, "Return kConnectTimeoutMilliseconds"},
    {"kLocalSchedulerFetchTimeoutMilliseconds", (PyCFunction) PyRayConfig_kLocalSchedulerFetchTimeoutMilliseconds, METH_NOARGS, "Return kLocalSchedulerFetchTimeoutMilliseconds"},
    {"kLocalSchedulerReconstructionTimeoutMilliseconds", (PyCFunction) PyRayConfig_kLocalSchedulerReconstructionTimeoutMilliseconds, METH_NOARGS, "Return kLocalSchedulerReconstructionTimeoutMilliseconds"},
    {"kKillWorkerTimeoutMilliseconds", (PyCFunction) PyRayConfig_kKillWorkerTimeoutMilliseconds, METH_NOARGS, "Return kKillWorkerTimeoutMilliseconds"},
    {"kDefaultNumCPUs", (PyCFunction) PyRayConfig_kDefaultNumCPUs, METH_NOARGS, "Return kDefaultNumCPUs"},
    {"kDefaultNumGPUs", (PyCFunction) PyRayConfig_kDefaultNumGPUs, METH_NOARGS, "Return kDefaultNumGPUs"},
    {"kDefaultNumCustomResource", (PyCFunction) PyRayConfig_kDefaultNumCustomResource, METH_NOARGS, "Return kDefaultNumCustomResource"},
    {"kManagerTimeoutMilliseconds", (PyCFunction) PyRayConfig_kManagerTimeoutMilliseconds, METH_NOARGS, "Return kManagerTimeoutMilliseconds"},
    {"kBufSize", (PyCFunction) PyRayConfig_kBufSize, METH_NOARGS, "Return kBufSize"},
    {"kMaxTimeForHandlerMilliseconds", (PyCFunction) PyRayConfig_kMaxTimeForHandlerMilliseconds, METH_NOARGS, "Return kMaxTimeForHandlerMilliseconds"},
    {"kSizeLimit", (PyCFunction) PyRayConfig_kSizeLimit, METH_NOARGS, "Return kSizeLimit"},
    {"kNumElementsLimit", (PyCFunction) PyRayConfig_kNumElementsLimit, METH_NOARGS, "Return kNumElementsLimit"},
    {"kMaxTimeForLoop", (PyCFunction) PyRayConfig_kMaxTimeForLoop, METH_NOARGS, "Return kMaxTimeForLoop"},
    {"kRedisDBConnectRetries", (PyCFunction) PyRayConfig_kRedisDBConnectRetries, METH_NOARGS, "Return kRedisDBConnectRetries"},
    {"kRedisDBConnectWaitMilliseconds", (PyCFunction) PyRayConfig_kRedisDBConnectWaitMilliseconds, METH_NOARGS, "Return kRedisDBConnectWaitMilliseconds"},
    {"kPlasmaDefaultReleaseDelay", (PyCFunction) PyRayConfig_kPlasmaDefaultReleaseDelay, METH_NOARGS, "Return kPlasmaDefaultReleaseDelay"},
    {"kL3CacheSizeBytes", (PyCFunction) PyRayConfig_kL3CacheSizeBytes, METH_NOARGS, "Return kL3CacheSizeBytes"},
    {NULL} /* Sentinel */
};

#endif /* CONFIG_EXTENSION_H */
