#include <Python.h>
#include "bytesobject.h"

#include "common.h"
#include "config_extension.h"

PyObject *PyRayConfig_make() {
  PyRayConfig *result = PyObject_New(PyRayConfig, &PyRayConfigType);
  result = (PyRayConfig *) PyObject_Init((PyObject *) result, &PyRayConfigType);
  return (PyObject *) result;
}

PyObject *PyRayConfig_kRayProtocolVersion(PyObject *self) {
  return PyLong_FromLongLong(kRayProtocolVersion);
}

PyObject *PyRayConfig_kHeartbeatTimeoutMilliseconds(PyObject *self) {
  return PyLong_FromLongLong(kHeartbeatTimeoutMilliseconds);
}

PyObject *PyRayConfig_kNumHeartbeatsTimeout(PyObject *self) {
  return PyLong_FromLongLong(kNumHeartbeatsTimeout);
}

PyObject *PyRayConfig_kGetTimeoutMilliseconds(PyObject *self) {
  return PyLong_FromLongLong(kGetTimeoutMilliseconds);
}

PyObject *PyRayConfig_kNumBindAttempts(PyObject *self) {
  return PyLong_FromLongLong(kNumBindAttempts);
}

PyObject *PyRayConfig_kBindTimeoutMilliseconds(PyObject *self) {
  return PyLong_FromLongLong(kBindTimeoutMilliseconds);
}

PyObject *PyRayConfig_kNumConnectAttempts(PyObject *self) {
  return PyLong_FromLongLong(kNumConnectAttempts);
}

PyObject *PyRayConfig_kConnectTimeoutMilliseconds(PyObject *self) {
  return PyLong_FromLongLong(kConnectTimeoutMilliseconds);
}

PyObject *PyRayConfig_kLocalSchedulerFetchTimeoutMilliseconds(PyObject *self) {
  return PyLong_FromLongLong(kLocalSchedulerFetchTimeoutMilliseconds);
}

PyObject *PyRayConfig_kLocalSchedulerReconstructionTimeoutMilliseconds(PyObject *self) {
  return PyLong_FromLongLong(kLocalSchedulerReconstructionTimeoutMilliseconds);
}

PyObject *PyRayConfig_kKillWorkerTimeoutMilliseconds(PyObject *self) {
  return PyLong_FromLongLong(kKillWorkerTimeoutMilliseconds);
}

PyObject *PyRayConfig_kDefaultNumCPUs(PyObject *self) {
  return PyFloat_FromDouble(kDefaultNumCPUs);
}

PyObject *PyRayConfig_kDefaultNumGPUs(PyObject *self) {
  return PyFloat_FromDouble(kDefaultNumGPUs);
}

PyObject *PyRayConfig_kDefaultNumCustomResource(PyObject *self) {
  return PyFloat_FromDouble(kDefaultNumCustomResource);
}

PyObject *PyRayConfig_kManagerTimeoutMilliseconds(PyObject *self) {
  return PyLong_FromLongLong(kManagerTimeoutMilliseconds);
}

PyObject *PyRayConfig_kBufSize(PyObject *self) {
  return PyLong_FromLongLong(kBufSize);
}

PyObject *PyRayConfig_kMaxTimeForHandlerMilliseconds(PyObject *self) {
  return PyLong_FromLongLong(kMaxTimeForHandlerMilliseconds);
}

PyObject *PyRayConfig_kSizeLimit(PyObject *self) {
  return PyLong_FromLongLong(kSizeLimit);
}

PyObject *PyRayConfig_kNumElementsLimit(PyObject *self) {
  return PyLong_FromLongLong(kNumElementsLimit);
}

PyObject *PyRayConfig_kMaxTimeForLoop(PyObject *self) {
  return PyLong_FromLongLong(kMaxTimeForLoop);
}

PyObject *PyRayConfig_kRedisDBConnectRetries(PyObject *self) {
  return PyLong_FromLongLong(kRedisDBConnectRetries);
}

PyObject *PyRayConfig_kRedisDBConnectWaitMilliseconds(PyObject *self) {
  return PyLong_FromLongLong(kRedisDBConnectWaitMilliseconds);
}

PyObject *PyRayConfig_kPlasmaDefaultReleaseDelay(PyObject *self) {
  return PyLong_FromLongLong(kPlasmaDefaultReleaseDelay);
}

PyObject *PyRayConfig_kL3CacheSizeBytes(PyObject *self) {
  return PyLong_FromLongLong(kL3CacheSizeBytes);
}

PyTypeObject PyRayConfigType = {
    PyVarObject_HEAD_INIT(NULL, 0) /* ob_size */
    "common.RayConfig",            /* tp_name */
    sizeof(PyRayConfig),           /* tp_basicsize */
    0,                             /* tp_itemsize */
    0,                             /* tp_dealloc */
    0,                             /* tp_print */
    0,                             /* tp_getattr */
    0,                             /* tp_setattr */
    0,                             /* tp_compare */
    0,                             /* tp_repr */
    0,                             /* tp_as_number */
    0,                             /* tp_as_sequence */
    0,                             /* tp_as_mapping */
    0,                             /* tp_hash */
    0,                             /* tp_call */
    0,                             /* tp_str */
    0,                             /* tp_getattro */
    0,                             /* tp_setattro */
    0,                             /* tp_as_buffer */
    Py_TPFLAGS_DEFAULT,            /* tp_flags */
    "RayConfig object",            /* tp_doc */
    0,                             /* tp_traverse */
    0,                             /* tp_clear */
    0,                             /* tp_richcompare */
    0,                             /* tp_weaklistoffset */
    0,                             /* tp_iter */
    0,                             /* tp_iternext */
    PyRayConfig_methods,           /* tp_methods */
    0,                             /* tp_members */
    0,                             /* tp_getset */
    0,                             /* tp_base */
    0,                             /* tp_dict */
    0,                             /* tp_descr_get */
    0,                             /* tp_descr_set */
    0,                             /* tp_dictoffset */
    0,                             /* tp_init */
    0,                             /* tp_alloc */
    PyType_GenericNew,             /* tp_new */
};
