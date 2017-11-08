#include <Python.h>
#include "bytesobject.h"

#include "common.h"
#include "config_extension.h"

PyObject *PyRayConfig_make() {
  PyRayConfig *result = PyObject_New(PyRayConfig, &PyRayConfigType);
  result = (PyRayConfig *) PyObject_Init((PyObject *) result, &PyRayConfigType);
  return (PyObject *) result;
}

PyObject *PyRayConfig_RAY_PROTOCOL_VERSION(PyObject *self) {
  return PyLong_FromLongLong(RAY_PROTOCOL_VERSION);
}

PyObject *PyRayConfig_HEARTBEAT_TIMEOUT_MILLISECONDS(PyObject *self) {
  return PyLong_FromLongLong(HEARTBEAT_TIMEOUT_MILLISECONDS);
}

PyObject *PyRayConfig_NUM_HEARTBEATS_TIMEOUT(PyObject *self) {
  return PyLong_FromLongLong(NUM_HEARTBEATS_TIMEOUT);
}

PyObject *PyRayConfig_GET_TIMEOUT_MILLISECONDS(PyObject *self) {
  return PyLong_FromLongLong(GET_TIMEOUT_MILLISECONDS);
}

PyObject *PyRayConfig_NUM_BIND_ATTEMPTS(PyObject *self) {
  return PyLong_FromLongLong(NUM_BIND_ATTEMPTS);
}

PyObject *PyRayConfig_BIND_TIMEOUT_MS(PyObject *self) {
  return PyLong_FromLongLong(BIND_TIMEOUT_MS);
}

PyObject *PyRayConfig_NUM_CONNECT_ATTEMPTS(PyObject *self) {
  return PyLong_FromLongLong(NUM_CONNECT_ATTEMPTS);
}

PyObject *PyRayConfig_CONNECT_TIMEOUT_MS(PyObject *self) {
  return PyLong_FromLongLong(CONNECT_TIMEOUT_MS);
}

PyObject *PyRayConfig_kLocalSchedulerFetchTimeoutMilliseconds(PyObject *self) {
  return PyLong_FromLongLong(kLocalSchedulerFetchTimeoutMilliseconds);
}

PyObject *PyRayConfig_kLocalSchedulerReconstructionTimeoutMilliseconds(PyObject *self) {
  return PyLong_FromLongLong(kLocalSchedulerReconstructionTimeoutMilliseconds);
}

PyObject *PyRayConfig_KILL_WORKER_TIMEOUT_MILLISECONDS(PyObject *self) {
  return PyLong_FromLongLong(KILL_WORKER_TIMEOUT_MILLISECONDS);
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

PyObject *PyRayConfig_MANAGER_TIMEOUT(PyObject *self) {
  return PyLong_FromLongLong(MANAGER_TIMEOUT);
}

PyObject *PyRayConfig_BUFSIZE(PyObject *self) {
  return PyLong_FromLongLong(BUFSIZE);
}

PyObject *PyRayConfig_max_time_for_handler(PyObject *self) {
  return PyLong_FromLongLong(max_time_for_handler);
}

PyObject *PyRayConfig_SIZE_LIMIT(PyObject *self) {
  return PyLong_FromLongLong(SIZE_LIMIT);
}

PyObject *PyRayConfig_NUM_ELEMENTS_LIMIT(PyObject *self) {
  return PyLong_FromLongLong(NUM_ELEMENTS_LIMIT);
}

PyObject *PyRayConfig_max_time_for_loop(PyObject *self) {
  return PyLong_FromLongLong(max_time_for_loop);
}

PyObject *PyRayConfig_REDIS_DB_CONNECT_RETRIES(PyObject *self) {
  return PyLong_FromLongLong(REDIS_DB_CONNECT_RETRIES);
}

PyObject *PyRayConfig_REDIS_DB_CONNECT_WAIT_MS(PyObject *self) {
  return PyLong_FromLongLong(REDIS_DB_CONNECT_WAIT_MS);
}

PyObject *PyRayConfig_PLASMA_DEFAULT_RELEASE_DELAY(PyObject *self) {
  return PyLong_FromLongLong(PLASMA_DEFAULT_RELEASE_DELAY);
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
