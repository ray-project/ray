#include <Python.h>
#include "bytesobject.h"

#include "common.h"

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
