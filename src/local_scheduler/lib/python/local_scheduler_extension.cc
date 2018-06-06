#include <Python.h>

#include "common_extension.h"
#include "config_extension.h"
#include "local_scheduler_client.h"
#include "task.h"

PyObject *LocalSchedulerError;

// clang-format off
typedef struct {
  PyObject_HEAD
  LocalSchedulerConnection *local_scheduler_connection;
} PyLocalSchedulerClient;
// clang-format on

static int PyLocalSchedulerClient_init(PyLocalSchedulerClient *self,
                                       PyObject *args,
                                       PyObject *kwds) {
  char *socket_name;
  UniqueID client_id;
  PyObject *is_worker;
  if (!PyArg_ParseTuple(args, "sO&O", &socket_name, PyStringToUniqueID,
                        &client_id, &is_worker)) {
    self->local_scheduler_connection = NULL;
    return -1;
  }
  /* Connect to the local scheduler. */
  self->local_scheduler_connection = LocalSchedulerConnection_init(
      socket_name, client_id, (bool) PyObject_IsTrue(is_worker));
  return 0;
}

static void PyLocalSchedulerClient_dealloc(PyLocalSchedulerClient *self) {
  if (self->local_scheduler_connection != NULL) {
    LocalSchedulerConnection_free(self->local_scheduler_connection);
  }
  Py_TYPE(self)->tp_free((PyObject *) self);
}

static PyObject *PyLocalSchedulerClient_disconnect(PyObject *self) {
  local_scheduler_disconnect_client(
      ((PyLocalSchedulerClient *) self)->local_scheduler_connection);
  Py_RETURN_NONE;
}

static PyObject *PyLocalSchedulerClient_submit(PyObject *self, PyObject *args) {
  PyObject *py_task;
  if (!PyArg_ParseTuple(args, "O", &py_task)) {
    return NULL;
  }
  LocalSchedulerConnection *connection =
      reinterpret_cast<PyLocalSchedulerClient *>(self)
          ->local_scheduler_connection;
  PyTask *task = reinterpret_cast<PyTask *>(py_task);

  if (!use_raylet(task)) {
    TaskExecutionSpec execution_spec = TaskExecutionSpec(
        *task->execution_dependencies, task->spec, task->size);
    local_scheduler_submit(connection, execution_spec);
  } else {
    local_scheduler_submit_raylet(connection, *task->execution_dependencies,
                                  *task->task_spec);
  }

  Py_RETURN_NONE;
}

// clang-format off
static PyObject *PyLocalSchedulerClient_get_task(PyObject *self) {
  TaskSpec *task_spec;
  int64_t task_size;
  /* Drop the global interpreter lock while we get a task because
   * local_scheduler_get_task may block for a long time. */
  Py_BEGIN_ALLOW_THREADS
  task_spec = local_scheduler_get_task(
      ((PyLocalSchedulerClient *) self)->local_scheduler_connection,
      &task_size);
  Py_END_ALLOW_THREADS
  return PyTask_make(task_spec, task_size);
}
// clang-format on

static PyObject *PyLocalSchedulerClient_reconstruct_object(PyObject *self,
                                                           PyObject *args) {
  ObjectID object_id;
  if (!PyArg_ParseTuple(args, "O&", PyStringToUniqueID, &object_id)) {
    return NULL;
  }
  local_scheduler_reconstruct_object(
      ((PyLocalSchedulerClient *) self)->local_scheduler_connection, object_id);
  Py_RETURN_NONE;
}

static PyObject *PyLocalSchedulerClient_log_event(PyObject *self,
                                                  PyObject *args) {
  const char *key;
  int key_length;
  const char *value;
  int value_length;
  double timestamp;
  if (!PyArg_ParseTuple(args, "s#s#d", &key, &key_length, &value, &value_length,
                        &timestamp)) {
    return NULL;
  }
  local_scheduler_log_event(
      ((PyLocalSchedulerClient *) self)->local_scheduler_connection,
      (uint8_t *) key, key_length, (uint8_t *) value, value_length, timestamp);
  Py_RETURN_NONE;
}

static PyObject *PyLocalSchedulerClient_notify_unblocked(PyObject *self) {
  local_scheduler_notify_unblocked(
      ((PyLocalSchedulerClient *) self)->local_scheduler_connection);
  Py_RETURN_NONE;
}

static PyObject *PyLocalSchedulerClient_compute_put_id(PyObject *self,
                                                       PyObject *args) {
  int put_index;
  TaskID task_id;
  PyObject *use_raylet;
  if (!PyArg_ParseTuple(args, "O&iO", &PyObjectToUniqueID, &task_id, &put_index,
                        &use_raylet)) {
    return NULL;
  }
  ObjectID put_id;
  if (!PyObject_IsTrue(use_raylet)) {
    put_id = task_compute_put_id(task_id, put_index);
    local_scheduler_put_object(
        ((PyLocalSchedulerClient *) self)->local_scheduler_connection, task_id,
        put_id);
  } else {
    // TODO(rkn): Raise an exception if the put index is not a valid value
    // instead of crashing in ComputePutId.
    put_id = ray::ComputePutId(task_id, put_index);
  }
  return PyObjectID_make(put_id);
}

static PyObject *PyLocalSchedulerClient_gpu_ids(PyObject *self) {
  /* Construct a Python list of GPU IDs. */
  std::vector<int> gpu_ids =
      ((PyLocalSchedulerClient *) self)->local_scheduler_connection->gpu_ids;
  int num_gpu_ids = gpu_ids.size();
  PyObject *gpu_ids_list = PyList_New((Py_ssize_t) num_gpu_ids);
  for (int i = 0; i < num_gpu_ids; ++i) {
    PyList_SetItem(gpu_ids_list, i, PyLong_FromLong(gpu_ids[i]));
  }
  return gpu_ids_list;
}

static PyObject *PyLocalSchedulerClient_get_actor_frontier(PyObject *self,
                                                           PyObject *args) {
  ActorID actor_id;
  if (!PyArg_ParseTuple(args, "O&", &PyObjectToUniqueID, &actor_id)) {
    return NULL;
  }

  auto frontier = local_scheduler_get_actor_frontier(
      ((PyLocalSchedulerClient *) self)->local_scheduler_connection, actor_id);
  return PyBytes_FromStringAndSize(
      reinterpret_cast<const char *>(frontier.data()), frontier.size());
}

static PyObject *PyLocalSchedulerClient_set_actor_frontier(PyObject *self,
                                                           PyObject *args) {
  PyObject *py_frontier;
  if (!PyArg_ParseTuple(args, "O", &py_frontier)) {
    return NULL;
  }

  std::vector<uint8_t> frontier;
  Py_ssize_t length = PyBytes_Size(py_frontier);
  char *frontier_data = PyBytes_AsString(py_frontier);
  frontier.assign(frontier_data, frontier_data + length);
  local_scheduler_set_actor_frontier(
      ((PyLocalSchedulerClient *) self)->local_scheduler_connection, frontier);
  Py_RETURN_NONE;
}

static PyObject *PyLocalSchedulerClient_wait(PyObject *self, PyObject *args) {
  PyObject *py_object_ids;
  int num_returns;
  int64_t timeout_ms;
  PyObject *py_wait_local;

  if (!PyArg_ParseTuple(args, "OilO", &py_object_ids, &num_returns, &timeout_ms,
                        &py_wait_local)) {
    return NULL;
  }

  bool wait_local = PyObject_IsTrue(py_wait_local);

  // Convert object ids.
  PyObject *iter = PyObject_GetIter(py_object_ids);
  if (!iter) {
    return NULL;
  }
  std::vector<ObjectID> object_ids;
  while (true) {
    PyObject *next = PyIter_Next(iter);
    ObjectID object_id;
    if (!next) {
      break;
    }
    if (!PyObjectToUniqueID(next, &object_id)) {
      // Error parsing object id.
      return NULL;
    }
    object_ids.push_back(object_id);
  }

  // Invoke wait.
  std::pair<std::vector<ObjectID>, std::vector<ObjectID>> result =
      local_scheduler_wait(reinterpret_cast<PyLocalSchedulerClient *>(self)
                               ->local_scheduler_connection,
                           object_ids, num_returns, timeout_ms,
                           static_cast<bool>(wait_local));

  // Convert result to py object.
  PyObject *py_found = PyList_New(static_cast<Py_ssize_t>(result.first.size()));
  for (uint i = 0; i < result.first.size(); ++i) {
    PyList_SetItem(py_found, i, PyObjectID_make(result.first[i]));
  }
  PyObject *py_remaining =
      PyList_New(static_cast<Py_ssize_t>(result.second.size()));
  for (uint i = 0; i < result.second.size(); ++i) {
    PyList_SetItem(py_remaining, i, PyObjectID_make(result.second[i]));
  }
  return Py_BuildValue("(OO)", py_found, py_remaining);
}

static PyMethodDef PyLocalSchedulerClient_methods[] = {
    {"disconnect", (PyCFunction) PyLocalSchedulerClient_disconnect, METH_NOARGS,
     "Notify the local scheduler that this client is exiting gracefully."},
    {"submit", (PyCFunction) PyLocalSchedulerClient_submit, METH_VARARGS,
     "Submit a task to the local scheduler."},
    {"get_task", (PyCFunction) PyLocalSchedulerClient_get_task, METH_NOARGS,
     "Get a task from the local scheduler."},
    {"reconstruct_object",
     (PyCFunction) PyLocalSchedulerClient_reconstruct_object, METH_VARARGS,
     "Ask the local scheduler to reconstruct an object."},
    {"log_event", (PyCFunction) PyLocalSchedulerClient_log_event, METH_VARARGS,
     "Log an event to the event log through the local scheduler."},
    {"notify_unblocked", (PyCFunction) PyLocalSchedulerClient_notify_unblocked,
     METH_NOARGS, "Notify the local scheduler that we are unblocked."},
    {"compute_put_id", (PyCFunction) PyLocalSchedulerClient_compute_put_id,
     METH_VARARGS, "Return the object ID for a put call within a task."},
    {"gpu_ids", (PyCFunction) PyLocalSchedulerClient_gpu_ids, METH_NOARGS,
     "Get the IDs of the GPUs that are reserved for this client."},
    {"get_actor_frontier",
     (PyCFunction) PyLocalSchedulerClient_get_actor_frontier, METH_VARARGS, ""},
    {"set_actor_frontier",
     (PyCFunction) PyLocalSchedulerClient_set_actor_frontier, METH_VARARGS, ""},
    {"wait", (PyCFunction) PyLocalSchedulerClient_wait, METH_VARARGS,
     "Wait for a list of objects to be created."},
    {NULL} /* Sentinel */
};

static PyTypeObject PyLocalSchedulerClientType = {
    PyVarObject_HEAD_INIT(NULL, 0)               /* ob_size */
    "local_scheduler.LocalSchedulerClient",      /* tp_name */
    sizeof(PyLocalSchedulerClient),              /* tp_basicsize */
    0,                                           /* tp_itemsize */
    (destructor) PyLocalSchedulerClient_dealloc, /* tp_dealloc */
    0,                                           /* tp_print */
    0,                                           /* tp_getattr */
    0,                                           /* tp_setattr */
    0,                                           /* tp_compare */
    0,                                           /* tp_repr */
    0,                                           /* tp_as_number */
    0,                                           /* tp_as_sequence */
    0,                                           /* tp_as_mapping */
    0,                                           /* tp_hash */
    0,                                           /* tp_call */
    0,                                           /* tp_str */
    0,                                           /* tp_getattro */
    0,                                           /* tp_setattro */
    0,                                           /* tp_as_buffer */
    Py_TPFLAGS_DEFAULT,                          /* tp_flags */
    "LocalSchedulerClient object",               /* tp_doc */
    0,                                           /* tp_traverse */
    0,                                           /* tp_clear */
    0,                                           /* tp_richcompare */
    0,                                           /* tp_weaklistoffset */
    0,                                           /* tp_iter */
    0,                                           /* tp_iternext */
    PyLocalSchedulerClient_methods,              /* tp_methods */
    0,                                           /* tp_members */
    0,                                           /* tp_getset */
    0,                                           /* tp_base */
    0,                                           /* tp_dict */
    0,                                           /* tp_descr_get */
    0,                                           /* tp_descr_set */
    0,                                           /* tp_dictoffset */
    (initproc) PyLocalSchedulerClient_init,      /* tp_init */
    0,                                           /* tp_alloc */
    PyType_GenericNew,                           /* tp_new */
};

static PyMethodDef local_scheduler_methods[] = {
    {"check_simple_value", check_simple_value, METH_VARARGS,
     "Should the object be passed by value?"},
    {"task_from_string", PyTask_from_string, METH_VARARGS,
     "Creates a Python PyTask object from a string representation of "
     "TaskSpec."},
    {"task_to_string", PyTask_to_string, METH_VARARGS,
     "Translates a PyTask python object to a byte string."},
    {NULL} /* Sentinel */
};

#if PY_MAJOR_VERSION >= 3
static struct PyModuleDef moduledef = {
    PyModuleDef_HEAD_INIT,
    "liblocal_scheduler",                /* m_name */
    "A module for the local scheduler.", /* m_doc */
    0,                                   /* m_size */
    local_scheduler_methods,             /* m_methods */
    NULL,                                /* m_reload */
    NULL,                                /* m_traverse */
    NULL,                                /* m_clear */
    NULL,                                /* m_free */
};
#endif

#if PY_MAJOR_VERSION >= 3
#define INITERROR return NULL
#else
#define INITERROR return
#endif

#ifndef PyMODINIT_FUNC /* declarations for DLL import/export */
#define PyMODINIT_FUNC void
#endif

#if PY_MAJOR_VERSION >= 3
#define MOD_INIT(name) PyMODINIT_FUNC PyInit_##name(void)
#else
#define MOD_INIT(name) PyMODINIT_FUNC init##name(void)
#endif

MOD_INIT(liblocal_scheduler_library_python) {
  if (PyType_Ready(&PyTaskType) < 0) {
    INITERROR;
  }

  if (PyType_Ready(&PyObjectIDType) < 0) {
    INITERROR;
  }

  if (PyType_Ready(&PyLocalSchedulerClientType) < 0) {
    INITERROR;
  }

  if (PyType_Ready(&PyRayConfigType) < 0) {
    INITERROR;
  }

#if PY_MAJOR_VERSION >= 3
  PyObject *m = PyModule_Create(&moduledef);
#else
  PyObject *m = Py_InitModule3("liblocal_scheduler_library_python",
                               local_scheduler_methods,
                               "A module for the local scheduler.");
#endif

  init_numpy_module();
  init_pickle_module();

  Py_INCREF(&PyTaskType);
  PyModule_AddObject(m, "Task", (PyObject *) &PyTaskType);

  Py_INCREF(&PyObjectIDType);
  PyModule_AddObject(m, "ObjectID", (PyObject *) &PyObjectIDType);

  Py_INCREF(&PyLocalSchedulerClientType);
  PyModule_AddObject(m, "LocalSchedulerClient",
                     (PyObject *) &PyLocalSchedulerClientType);

  g_task_builder = make_task_builder();

  char common_error[] = "common.error";
  CommonError = PyErr_NewException(common_error, NULL, NULL);
  Py_INCREF(CommonError);
  PyModule_AddObject(m, "common_error", CommonError);

  Py_INCREF(&PyRayConfigType);
  PyModule_AddObject(m, "RayConfig", (PyObject *) &PyRayConfigType);

  /* Create the global config object. */
  PyObject *config = PyRayConfig_make();
  /* TODO(rkn): Do we need Py_INCREF(config)? */
  PyModule_AddObject(m, "_config", config);

#if PY_MAJOR_VERSION >= 3
  return m;
#endif
}
