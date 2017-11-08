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

PyObject *PyRayConfig_ray_protocol_version(PyObject *self);
PyObject *PyRayConfig_heartbeat_timeout_milliseconds(PyObject *self);
PyObject *PyRayConfig_num_heartbeats_timeout(PyObject *self);
PyObject *PyRayConfig_get_timeout_milliseconds(PyObject *self);
PyObject *PyRayConfig_worker_get_request_size(PyObject *self);
PyObject *PyRayConfig_worker_fetch_request_size(PyObject *self);
PyObject *PyRayConfig_num_connect_attempts(PyObject *self);
PyObject *PyRayConfig_connect_timeout_milliseconds(PyObject *self);
PyObject *PyRayConfig_local_scheduler_fetch_timeout_milliseconds(
    PyObject *self);
PyObject *PyRayConfig_local_scheduler_reconstruction_timeout_milliseconds(
    PyObject *self);
PyObject *PyRayConfig_max_num_to_reconstruct(PyObject *self);
PyObject *PyRayConfig_local_scheduler_fetch_request_size(PyObject *self);
PyObject *PyRayConfig_kill_worker_timeout_milliseconds(PyObject *self);
PyObject *PyRayConfig_default_num_CPUs(PyObject *self);
PyObject *PyRayConfig_default_num_GPUs(PyObject *self);
PyObject *PyRayConfig_default_num_custom_resource(PyObject *self);
PyObject *PyRayConfig_manager_timeout_milliseconds(PyObject *self);
PyObject *PyRayConfig_buf_size(PyObject *self);
PyObject *PyRayConfig_max_time_for_handler_milliseconds(PyObject *self);
PyObject *PyRayConfig_size_limit(PyObject *self);
PyObject *PyRayConfig_num_elements_limit(PyObject *self);
PyObject *PyRayConfig_max_time_for_loop(PyObject *self);
PyObject *PyRayConfig_redis_db_connect_retries(PyObject *self);
PyObject *PyRayConfig_redis_db_connect_wait_milliseconds(PyObject *self);
PyObject *PyRayConfig_plasma_default_release_delay(PyObject *self);
PyObject *PyRayConfig_L3_cache_size_bytes(PyObject *self);

#endif /* CONFIG_EXTENSION_H */
