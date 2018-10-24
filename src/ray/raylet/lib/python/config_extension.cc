#include <Python.h>
#include "bytesobject.h"

#include "ray/ray_config.h"
#include "config_extension.h"

PyObject *PyRayConfig_make() {
  PyRayConfig *result = PyObject_New(PyRayConfig, &PyRayConfigType);
  result = (PyRayConfig *) PyObject_Init((PyObject *) result, &PyRayConfigType);
  return (PyObject *) result;
}

PyObject *PyRayConfig_ray_protocol_version(PyObject *self) {
  return PyLong_FromLongLong(RayConfig::instance().ray_protocol_version());
}

PyObject *PyRayConfig_heartbeat_timeout_milliseconds(PyObject *self) {
  return PyLong_FromLongLong(
      RayConfig::instance().heartbeat_timeout_milliseconds());
}

PyObject *PyRayConfig_num_heartbeats_timeout(PyObject *self) {
  return PyLong_FromLongLong(RayConfig::instance().num_heartbeats_timeout());
}

PyObject *PyRayConfig_get_timeout_milliseconds(PyObject *self) {
  return PyLong_FromLongLong(RayConfig::instance().get_timeout_milliseconds());
}

PyObject *PyRayConfig_worker_get_request_size(PyObject *self) {
  return PyLong_FromLongLong(RayConfig::instance().worker_get_request_size());
}

PyObject *PyRayConfig_worker_fetch_request_size(PyObject *self) {
  return PyLong_FromLongLong(RayConfig::instance().worker_fetch_request_size());
}

PyObject *PyRayConfig_actor_max_dummy_objects(PyObject *self) {
  return PyLong_FromLongLong(RayConfig::instance().actor_max_dummy_objects());
}

PyObject *PyRayConfig_num_connect_attempts(PyObject *self) {
  return PyLong_FromLongLong(RayConfig::instance().num_connect_attempts());
}

PyObject *PyRayConfig_connect_timeout_milliseconds(PyObject *self) {
  return PyLong_FromLongLong(
      RayConfig::instance().connect_timeout_milliseconds());
}

PyObject *PyRayConfig_local_scheduler_fetch_timeout_milliseconds(
    PyObject *self) {
  return PyLong_FromLongLong(
      RayConfig::instance().local_scheduler_fetch_timeout_milliseconds());
}

PyObject *PyRayConfig_local_scheduler_reconstruction_timeout_milliseconds(
    PyObject *self) {
  return PyLong_FromLongLong(
      RayConfig::instance()
          .local_scheduler_reconstruction_timeout_milliseconds());
}

PyObject *PyRayConfig_max_num_to_reconstruct(PyObject *self) {
  return PyLong_FromLongLong(RayConfig::instance().max_num_to_reconstruct());
}

PyObject *PyRayConfig_local_scheduler_fetch_request_size(PyObject *self) {
  return PyLong_FromLongLong(
      RayConfig::instance().local_scheduler_fetch_request_size());
}

PyObject *PyRayConfig_kill_worker_timeout_milliseconds(PyObject *self) {
  return PyLong_FromLongLong(
      RayConfig::instance().kill_worker_timeout_milliseconds());
}

PyObject *PyRayConfig_manager_timeout_milliseconds(PyObject *self) {
  return PyLong_FromLongLong(
      RayConfig::instance().manager_timeout_milliseconds());
}

PyObject *PyRayConfig_buf_size(PyObject *self) {
  return PyLong_FromLongLong(RayConfig::instance().buf_size());
}

PyObject *PyRayConfig_max_time_for_handler_milliseconds(PyObject *self) {
  return PyLong_FromLongLong(
      RayConfig::instance().max_time_for_handler_milliseconds());
}

PyObject *PyRayConfig_size_limit(PyObject *self) {
  return PyLong_FromLongLong(RayConfig::instance().size_limit());
}

PyObject *PyRayConfig_num_elements_limit(PyObject *self) {
  return PyLong_FromLongLong(RayConfig::instance().num_elements_limit());
}

PyObject *PyRayConfig_max_time_for_loop(PyObject *self) {
  return PyLong_FromLongLong(RayConfig::instance().max_time_for_loop());
}

PyObject *PyRayConfig_redis_db_connect_retries(PyObject *self) {
  return PyLong_FromLongLong(RayConfig::instance().redis_db_connect_retries());
}

PyObject *PyRayConfig_redis_db_connect_wait_milliseconds(PyObject *self) {
  return PyLong_FromLongLong(
      RayConfig::instance().redis_db_connect_wait_milliseconds());
}

PyObject *PyRayConfig_plasma_default_release_delay(PyObject *self) {
  return PyLong_FromLongLong(
      RayConfig::instance().plasma_default_release_delay());
}

PyObject *PyRayConfig_L3_cache_size_bytes(PyObject *self) {
  return PyLong_FromLongLong(RayConfig::instance().L3_cache_size_bytes());
}

static PyMethodDef PyRayConfig_methods[] = {
    {"ray_protocol_version", (PyCFunction) PyRayConfig_ray_protocol_version,
     METH_NOARGS, "Return ray_protocol_version"},
    {"heartbeat_timeout_milliseconds",
     (PyCFunction) PyRayConfig_heartbeat_timeout_milliseconds, METH_NOARGS,
     "Return heartbeat_timeout_milliseconds"},
    {"num_heartbeats_timeout", (PyCFunction) PyRayConfig_num_heartbeats_timeout,
     METH_NOARGS, "Return num_heartbeats_timeout"},
    {"get_timeout_milliseconds",
     (PyCFunction) PyRayConfig_get_timeout_milliseconds, METH_NOARGS,
     "Return get_timeout_milliseconds"},
    {"worker_get_request_size",
     (PyCFunction) PyRayConfig_worker_get_request_size, METH_NOARGS,
     "Return worker_get_request_size"},
    {"worker_fetch_request_size",
     (PyCFunction) PyRayConfig_worker_fetch_request_size, METH_NOARGS,
     "Return worker_fetch_request_size"},
    {"actor_max_dummy_objects",
     (PyCFunction) PyRayConfig_actor_max_dummy_objects, METH_NOARGS,
     "Return actor_max_dummy_objects"},
    {"num_connect_attempts", (PyCFunction) PyRayConfig_num_connect_attempts,
     METH_NOARGS, "Return num_connect_attempts"},
    {"connect_timeout_milliseconds",
     (PyCFunction) PyRayConfig_connect_timeout_milliseconds, METH_NOARGS,
     "Return connect_timeout_milliseconds"},
    {"local_scheduler_fetch_timeout_milliseconds",
     (PyCFunction) PyRayConfig_local_scheduler_fetch_timeout_milliseconds,
     METH_NOARGS, "Return local_scheduler_fetch_timeout_milliseconds"},
    {"local_scheduler_reconstruction_timeout_milliseconds",
     (PyCFunction)
         PyRayConfig_local_scheduler_reconstruction_timeout_milliseconds,
     METH_NOARGS, "Return local_scheduler_reconstruction_timeout_milliseconds"},
    {"max_num_to_reconstruct", (PyCFunction) PyRayConfig_max_num_to_reconstruct,
     METH_NOARGS, "Return max_num_to_reconstruct"},
    {"local_scheduler_fetch_request_size",
     (PyCFunction) PyRayConfig_local_scheduler_fetch_request_size, METH_NOARGS,
     "Return local_scheduler_fetch_request_size"},
    {"kill_worker_timeout_milliseconds",
     (PyCFunction) PyRayConfig_kill_worker_timeout_milliseconds, METH_NOARGS,
     "Return kill_worker_timeout_milliseconds"},
    {"manager_timeout_milliseconds",
     (PyCFunction) PyRayConfig_manager_timeout_milliseconds, METH_NOARGS,
     "Return manager_timeout_milliseconds"},
    {"buf_size", (PyCFunction) PyRayConfig_buf_size, METH_NOARGS,
     "Return buf_size"},
    {"max_time_for_handler_milliseconds",
     (PyCFunction) PyRayConfig_max_time_for_handler_milliseconds, METH_NOARGS,
     "Return max_time_for_handler_milliseconds"},
    {"size_limit", (PyCFunction) PyRayConfig_size_limit, METH_NOARGS,
     "Return size_limit"},
    {"num_elements_limit", (PyCFunction) PyRayConfig_num_elements_limit,
     METH_NOARGS, "Return num_elements_limit"},
    {"max_time_for_loop", (PyCFunction) PyRayConfig_max_time_for_loop,
     METH_NOARGS, "Return max_time_for_loop"},
    {"redis_db_connect_retries",
     (PyCFunction) PyRayConfig_redis_db_connect_retries, METH_NOARGS,
     "Return redis_db_connect_retries"},
    {"redis_db_connect_wait_milliseconds",
     (PyCFunction) PyRayConfig_redis_db_connect_wait_milliseconds, METH_NOARGS,
     "Return redis_db_connect_wait_milliseconds"},
    {"plasma_default_release_delay",
     (PyCFunction) PyRayConfig_plasma_default_release_delay, METH_NOARGS,
     "Return plasma_default_release_delay"},
    {"L3_cache_size_bytes", (PyCFunction) PyRayConfig_L3_cache_size_bytes,
     METH_NOARGS, "Return L3_cache_size_bytes"},
    {NULL} /* Sentinel */
};

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
