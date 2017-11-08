#ifndef CONFIG_EXTENSION_H
#define CONFIG_EXTENSION_H

#include <Python.h>

#include "common.h"

// clang-format off
typedef struct {
  PyObject_HEAD
} PyRayConfig;
// clang-format on

PyObject *PyRayConfig_HEARTBEAT_TIMEOUT_MILLISECONDS(PyObject *self);

static PyMethodDef PyRayConfig_methods[] = {
    {"RAY_PROTOCOL_VERSION", (PyCFunction) PyRayConfig_RAY_PROTOCOL_VERSION, METH_NOARGS, "Return RAY_PROTOCOL_VERSION"},
    {PyRayConfig_HEARTBEAT_TIMEOUT_MILLISECONDS"", (PyCFunction) PyRayConfig_HEARTBEAT_TIMEOUT_MILLISECONDS, METH_NOARGS, "Return HEARTBEAT_TIMEOUT_MILLISECONDS"},
    {"NUM_HEARTBEATS_TIMEOUT", (PyCFunction) PyRayConfig_NUM_HEARTBEATS_TIMEOUT, METH_NOARGS, "Return NUM_HEARTBEATS_TIMEOUT"},
    {"GET_TIMEOUT_MILLISECONDS", (PyCFunction) PyRayConfig_GET_TIMEOUT_MILLISECONDS, METH_NOARGS, "Return GET_TIMEOUT_MILLISECONDS"},
    {"NUM_BIND_ATTEMPTS", (PyCFunction) PyRayConfig_NUM_BIND_ATTEMPTS, METH_NOARGS, "Return NUM_BIND_ATTEMPTS"},
    {"BIND_TIMEOUT_MS", (PyCFunction) PyRayConfig_BIND_TIMEOUT_MS, METH_NOARGS, "Return BIND_TIMEOUT_MS"},
    {"NUM_CONNECT_ATTEMPTS", (PyCFunction) PyRayConfig_NUM_CONNECT_ATTEMPTS, METH_NOARGS, "Return NUM_CONNECT_ATTEMPTS"},
    {"CONNECT_TIMEOUT_MS", (PyCFunction) PyRayConfig_CONNECT_TIMEOUT_MS, METH_NOARGS, "Return CONNECT_TIMEOUT_MS"},
    {"kLocalSchedulerFetchTimeoutMilliseconds", (PyCFunction) PyRayConfig_kLocalSchedulerFetchTimeoutMilliseconds, METH_NOARGS, "Return kLocalSchedulerFetchTimeoutMilliseconds"},
    {"kLocalSchedulerReconstructionTimeoutMilliseconds", (PyCFunction) PyRayConfig_kLocalSchedulerReconstructionTimeoutMilliseconds, METH_NOARGS, "Return kLocalSchedulerReconstructionTimeoutMilliseconds"},
    {"KILL_WORKER_TIMEOUT_MILLISECONDS", (PyCFunction) PyRayConfig_KILL_WORKER_TIMEOUT_MILLISECONDS, METH_NOARGS, "Return KILL_WORKER_TIMEOUT_MILLISECONDS"},
    {"kDefaultNumCPUs", (PyCFunction) PyRayConfig_kDefaultNumCPUs, METH_NOARGS, "Return kDefaultNumCPUs"},
    {"kDefaultNumGPUs", (PyCFunction) PyRayConfig_kDefaultNumGPUs, METH_NOARGS, "Return kDefaultNumGPUs"},
    {"kDefaultNumCustomResource", (PyCFunction) PyRayConfig_kDefaultNumCustomResource, METH_NOARGS, "Return kDefaultNumCustomResource"},
    {"MANAGER_TIMEOUT", (PyCFunction) PyRayConfig_MANAGER_TIMEOUT, METH_NOARGS, "Return MANAGER_TIMEOUT"},
    {"BUFSIZE", (PyCFunction) PyRayConfig_BUFSIZE, METH_NOARGS, "Return BUFSIZE"},
    {"max_time_for_handler", (PyCFunction) PyRayConfig_max_time_for_handler, METH_NOARGS, "Return max_time_for_handler"},
    {"SIZE_LIMIT", (PyCFunction) PyRayConfig_SIZE_LIMIT, METH_NOARGS, "Return SIZE_LIMIT"},
    {"NUM_ELEMENTS_LIMIT", (PyCFunction) PyRayConfig_NUM_ELEMENTS_LIMIT, METH_NOARGS, "Return NUM_ELEMENTS_LIMIT"},
    {"max_time_for_loop", (PyCFunction) PyRayConfig_max_time_for_loop, METH_NOARGS, "Return max_time_for_loop"},
    {"REDIS_DB_CONNECT_RETRIES", (PyCFunction) PyRayConfig_REDIS_DB_CONNECT_RETRIES, METH_NOARGS, "Return REDIS_DB_CONNECT_RETRIES"},
    {"REDIS_DB_CONNECT_WAIT_MS", (PyCFunction) PyRayConfig_REDIS_DB_CONNECT_WAIT_MS, METH_NOARGS, "Return REDIS_DB_CONNECT_WAIT_MS"},
    {"PLASMA_DEFAULT_RELEASE_DELAY", (PyCFunction) PyRayConfig_PLASMA_DEFAULT_RELEASE_DELAY, METH_NOARGS, "Return PLASMA_DEFAULT_RELEASE_DELAY"},
    {"kL3CacheSizeBytes", (PyCFunction) PyRayConfig_kL3CacheSizeBytes, METH_NOARGS, "Return kL3CacheSizeBytes"},
    {NULL} /* Sentinel */
};

static PyMemberDef PyRayConfig_members[] = {
    {NULL} /* Sentinel */
};

PyTypeObject PyRayConfigType = {
    PyVarObject_HEAD_INIT(NULL, 0)        /* ob_size */
    "common.RayConfig",                    /* tp_name */
    sizeof(PyRayConfig),                   /* tp_basicsize */
    0,                                    /* tp_itemsize */
    0,                                    /* tp_dealloc */
    0,                                    /* tp_print */
    0,                                    /* tp_getattr */
    0,                                    /* tp_setattr */
    0,                                    /* tp_compare */
    0,           /* tp_repr */
    0,                                    /* tp_as_number */
    0,                                    /* tp_as_sequence */
    0,                                    /* tp_as_mapping */
    0,           /* tp_hash */
    0,                                    /* tp_call */
    0,                                    /* tp_str */
    0,                                    /* tp_getattro */
    0,                                    /* tp_setattro */
    0,                                    /* tp_as_buffer */
    Py_TPFLAGS_DEFAULT,                   /* tp_flags */
    "RayConfig object",                    /* tp_doc */
    0,                                    /* tp_traverse */
    0,                                    /* tp_clear */
    0, /* tp_richcompare */
    0,                                    /* tp_weaklistoffset */
    0,                                    /* tp_iter */
    0,                                    /* tp_iternext */
    PyRayConfig_methods,                   /* tp_methods */
    PyRayConfig_members,                   /* tp_members */
    0,                                    /* tp_getset */
    0,                                    /* tp_base */
    0,                                    /* tp_dict */
    0,                                    /* tp_descr_get */
    0,                                    /* tp_descr_set */
    0,                                    /* tp_dictoffset */
    0,           /* tp_init */
    0,                                    /* tp_alloc */
    PyType_GenericNew,                    /* tp_new */
};

#endif /* CONFIG_EXTENSION_H */
