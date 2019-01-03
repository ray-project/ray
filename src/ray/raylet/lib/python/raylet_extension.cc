#include <Python.h>
#include <sstream>

#include "common_extension.h"
#include "config_extension.h"
#include "ray/raylet/raylet_client.h"

PyObject *LocalSchedulerError;

// clang-format off
typedef struct {
  PyObject_HEAD
  RayletClient *raylet_client;
} PyRayletClient;
// clang-format on

static int PyRayletClient_init(PyRayletClient *self, PyObject *args, PyObject *kwds) {
  char *socket_name;
  UniqueID client_id;
  PyObject *is_worker;
  JobID driver_id;
  if (!PyArg_ParseTuple(args, "sO&OO&", &socket_name, PyStringToUniqueID, &client_id,
                        &is_worker, &PyObjectToUniqueID, &driver_id)) {
    self->raylet_client = NULL;
    return -1;
  }
  /* Connect to the local scheduler. */
  self->raylet_client = new RayletClient(socket_name, client_id,
                                         static_cast<bool>(PyObject_IsTrue(is_worker)),
                                         driver_id, Language::PYTHON);
  return 0;
}

static void PyRayletClient_dealloc(PyRayletClient *self) {
  if (self->raylet_client != NULL) {
    delete self->raylet_client;
  }
  Py_TYPE(self)->tp_free((PyObject *)self);
}

static PyObject *PyRayletClient_Disconnect(PyRayletClient *self) {
  auto status = self->raylet_client->Disconnect();
  RAY_CHECK_OK_PREPEND(status, "[RayletClient] Failed to disconnect.");
  Py_RETURN_NONE;
}

static PyObject *PyRayletClient_SubmitTask(PyRayletClient *self, PyObject *args) {
  PyObject *py_task;
  if (!PyArg_ParseTuple(args, "O", &py_task)) {
    return NULL;
  }
  PyTask *task = reinterpret_cast<PyTask *>(py_task);
  auto status =
      self->raylet_client->SubmitTask(*task->execution_dependencies, *task->task_spec);
  RAY_CHECK_OK_PREPEND(status, "[RayletClient] Failed to submit a task to raylet.");
  Py_RETURN_NONE;
}

// clang-format off
static PyObject *PyRayletClient_GetTask(PyRayletClient *self) {
  std::unique_ptr<ray::raylet::TaskSpecification> task_spec;
  /* Drop the global interpreter lock while we get a task because
   * raylet_GetTask may block for a long time. */
  Py_BEGIN_ALLOW_THREADS
  auto status = self->raylet_client->GetTask(&task_spec);
  RAY_CHECK_OK_PREPEND(status, "[RayletClient] Failed to get a task from raylet.");
  Py_END_ALLOW_THREADS
  return PyTask_make(task_spec.release());
}
// clang-format on

/// A helper function that converts a Python list of object ids to a vector.
///
/// \param py_list The Python list of object ids.
/// \param output The output vector.
/// \return True if an error occurred when parsing the Python object ids, false otherwise.
bool py_object_id_list_to_vector(PyObject *py_list, std::vector<UniqueID> &output) {
  Py_ssize_t n = PyList_Size(py_list);
  for (int64_t i = 0; i < n; ++i) {
    ObjectID object_id;
    PyObject *py_object_id = PyList_GetItem(py_list, i);
    if (!PyObjectToUniqueID(py_object_id, &object_id)) {
      return true;
    }
    output.push_back(object_id);
  }
  return false;
}

static PyObject *PyRayletClient_FetchOrReconstruct(PyRayletClient *self, PyObject *args) {
  PyObject *py_object_ids;
  PyObject *py_fetch_only;
  TaskID current_task_id;
  if (!PyArg_ParseTuple(args, "OO|O&", &py_object_ids, &py_fetch_only,
                        &PyObjectToUniqueID, &current_task_id)) {
    return NULL;
  }
  bool fetch_only = PyObject_IsTrue(py_fetch_only);

  // Convert object ids.
  std::vector<ObjectID> object_ids;
  if (py_object_id_list_to_vector(py_object_ids, object_ids)) {
    return NULL;
  }

  auto status =
      self->raylet_client->FetchOrReconstruct(object_ids, fetch_only, current_task_id);
  if (status.ok()) {
    Py_RETURN_NONE;
  } else {
    std::ostringstream stream;
    stream << "[RayletClient] FetchOrReconstruct failed: "
           << "raylet client may be closed, check raylet status. error message: "
           << status.ToString();
    PyErr_SetString(CommonError, stream.str().c_str());
    return NULL;
  }
}

static PyObject *PyRayletClient_NotifyUnblocked(PyRayletClient *self, PyObject *args) {
  TaskID current_task_id;
  if (!PyArg_ParseTuple(args, "O&", &PyObjectToUniqueID, &current_task_id)) {
    return NULL;
  }
  auto status = self->raylet_client->NotifyUnblocked(current_task_id);
  RAY_CHECK_OK_PREPEND(status, "[RayletClient] Failed to notify unblocked.");
  Py_RETURN_NONE;
}

static PyObject *PyRayletClient_compute_put_id(PyObject *self, PyObject *args) {
  int put_index;
  TaskID task_id;
  if (!PyArg_ParseTuple(args, "O&i", &PyObjectToUniqueID, &task_id, &put_index)) {
    return NULL;
  }
  const ObjectID put_id = ComputePutId(task_id, put_index);
  return PyObjectID_make(put_id);
}

static PyObject *PyRayletClient_resource_ids(PyRayletClient *self) {
  // Construct a Python dictionary of resource IDs and resource fractions.
  PyObject *resource_ids = PyDict_New();
  for (auto const &resource_info : self->raylet_client->GetResourceIDs()) {
    auto const &resource_name = resource_info.first;
    auto const &ids_and_fractions = resource_info.second;

#if PY_MAJOR_VERSION >= 3
    PyObject *key =
        PyUnicode_FromStringAndSize(resource_name.data(), resource_name.size());
#else
    PyObject *key = PyBytes_FromStringAndSize(resource_name.data(), resource_name.size());
#endif
    PyObject *value = PyList_New(ids_and_fractions.size());
    for (size_t i = 0; i < ids_and_fractions.size(); ++i) {
      auto const &id_and_fraction = ids_and_fractions[i];
      PyObject *id_fraction_pair =
          Py_BuildValue("(Ld)", id_and_fraction.first, id_and_fraction.second);
      PyList_SetItem(value, i, id_fraction_pair);
    }
    PyDict_SetItem(resource_ids, key, value);
    Py_DECREF(key);
    Py_DECREF(value);
  }

  return resource_ids;
}

static PyObject *PyRayletClient_Wait(PyRayletClient *self, PyObject *args) {
  PyObject *py_object_ids;
  int num_returns;
  int64_t timeout_ms;
  PyObject *py_wait_local;
  TaskID current_task_id;

  if (!PyArg_ParseTuple(args, "OilOO&", &py_object_ids, &num_returns, &timeout_ms,
                        &py_wait_local, &PyObjectToUniqueID, &current_task_id)) {
    return NULL;
  }

  bool wait_local = PyObject_IsTrue(py_wait_local);

  // Convert object ids.
  std::vector<ObjectID> object_ids;
  if (py_object_id_list_to_vector(py_object_ids, object_ids)) {
    return NULL;
  }

  // Invoke wait.
  WaitResultPair result;
  auto status = self->raylet_client->Wait(object_ids, num_returns, timeout_ms, wait_local,
                                          current_task_id, &result);
  RAY_CHECK_OK_PREPEND(status, "[RayletClient] Failed to wait for objects.");

  // Convert result to py object.
  PyObject *py_found = PyList_New(static_cast<Py_ssize_t>(result.first.size()));
  for (uint i = 0; i < result.first.size(); ++i) {
    PyList_SetItem(py_found, i, PyObjectID_make(result.first[i]));
  }
  PyObject *py_remaining = PyList_New(static_cast<Py_ssize_t>(result.second.size()));
  for (uint i = 0; i < result.second.size(); ++i) {
    PyList_SetItem(py_remaining, i, PyObjectID_make(result.second[i]));
  }
  return Py_BuildValue("(NN)", py_found, py_remaining);
}

static PyObject *PyRayletClient_PushError(PyRayletClient *self, PyObject *args) {
  JobID job_id;
  const char *type;
  int type_length;
  const char *error_message;
  int error_message_length;
  double timestamp;
  if (!PyArg_ParseTuple(args, "O&s#s#d", &PyObjectToUniqueID, &job_id, &type,
                        &type_length, &error_message, &error_message_length,
                        &timestamp)) {
    return NULL;
  }

  auto status = self->raylet_client->PushError(
      job_id, std::string(type, type_length),
      std::string(error_message, error_message_length), timestamp);
  RAY_CHECK_OK_PREPEND(status, "[RayletClient] Failed to push errors to raylet.");
  Py_RETURN_NONE;
}

int PyBytes_or_PyUnicode_to_string(PyObject *py_string, std::string &out) {
  // Handle the case where the key is a bytes object and the case where it
  // is a unicode object.
  if (PyUnicode_Check(py_string)) {
    PyObject *ascii_string = PyUnicode_AsASCIIString(py_string);
    out = std::string(PyBytes_AsString(ascii_string), PyBytes_Size(ascii_string));
    Py_DECREF(ascii_string);
  } else if (PyBytes_Check(py_string)) {
    out = std::string(PyBytes_AsString(py_string), PyBytes_Size(py_string));
  } else {
    return -1;
  }

  return 0;
}

static PyObject *PyRayletClient_PushProfileEvents(PyRayletClient *self, PyObject *args) {
  const char *component_type;
  int component_type_length;
  UniqueID component_id;
  PyObject *profile_data;
  const char *node_ip_address;
  int node_ip_address_length;

  if (!PyArg_ParseTuple(args, "s#O&s#O", &component_type, &component_type_length,
                        &PyObjectToUniqueID, &component_id, &node_ip_address,
                        &node_ip_address_length, &profile_data)) {
    return NULL;
  }

  ProfileTableDataT profile_info;
  profile_info.component_type = std::string(component_type, component_type_length);
  profile_info.component_id = component_id.binary();
  profile_info.node_ip_address = std::string(node_ip_address, node_ip_address_length);

  if (PyList_Size(profile_data) == 0) {
    // Short circuit if there are no profile events.
    Py_RETURN_NONE;
  }

  for (int64_t i = 0; i < PyList_Size(profile_data); ++i) {
    ProfileEventT profile_event;
    PyObject *py_profile_event = PyList_GetItem(profile_data, i);

    if (!PyDict_CheckExact(py_profile_event)) {
      return NULL;
    }

    PyObject *key, *val;
    Py_ssize_t pos = 0;
    while (PyDict_Next(py_profile_event, &pos, &key, &val)) {
      std::string key_string;
      if (PyBytes_or_PyUnicode_to_string(key, key_string) == -1) {
        return NULL;
      }

      // TODO(rkn): If the dictionary is formatted incorrectly, that could lead
      // to errors. E.g., if any of the strings are empty, that will cause
      // segfaults in the node manager.

      if (key_string == std::string("event_type")) {
        if (PyBytes_or_PyUnicode_to_string(val, profile_event.event_type) == -1) {
          return NULL;
        }
        if (profile_event.event_type.size() == 0) {
          return NULL;
        }
      } else if (key_string == std::string("start_time")) {
        profile_event.start_time = PyFloat_AsDouble(val);
      } else if (key_string == std::string("end_time")) {
        profile_event.end_time = PyFloat_AsDouble(val);
      } else if (key_string == std::string("extra_data")) {
        if (PyBytes_or_PyUnicode_to_string(val, profile_event.extra_data) == -1) {
          return NULL;
        }
        if (profile_event.extra_data.size() == 0) {
          return NULL;
        }
      } else {
        return NULL;
      }
    }

    // Note that profile_info.profile_events is a vector of unique pointers, so
    // profile_event will be deallocated when profile_info goes out of scope.
    profile_info.profile_events.emplace_back(new ProfileEventT(profile_event));
  }

  auto status = self->raylet_client->PushProfileEvents(profile_info);
  RAY_CHECK_OK_PREPEND(status, "[RayletClient] Failed to push profile events to raylet.");
  Py_RETURN_NONE;
}

static PyObject *PyRayletClient_FreeObjects(PyRayletClient *self, PyObject *args) {
  PyObject *py_object_ids;
  PyObject *py_local_only;

  if (!PyArg_ParseTuple(args, "OO", &py_object_ids, &py_local_only)) {
    return NULL;
  }

  bool local_only = static_cast<bool>(PyObject_IsTrue(py_local_only));

  // Convert object ids.
  std::vector<ObjectID> object_ids;
  if (py_object_id_list_to_vector(py_object_ids, object_ids)) {
    return NULL;
  }

  // Invoke raylet_FreeObjects.
  auto status = self->raylet_client->FreeObjects(object_ids, local_only);
  RAY_CHECK_OK_PREPEND(status, "[RayletClient] Failed to free objects.");
  Py_RETURN_NONE;
}

static PyMethodDef PyRayletClient_methods[] = {
    {"disconnect", (PyCFunction)PyRayletClient_Disconnect, METH_NOARGS,
     "Notify the local scheduler that this client is exiting gracefully."},
    {"submit_task", (PyCFunction)PyRayletClient_SubmitTask, METH_VARARGS,
     "Submit a task to the local scheduler."},
    {"get_task", (PyCFunction)PyRayletClient_GetTask, METH_NOARGS,
     "Get a task from the local scheduler."},
    {"fetch_or_reconstruct", (PyCFunction)PyRayletClient_FetchOrReconstruct, METH_VARARGS,
     "Ask the local scheduler to reconstruct an object."},
    {"notify_unblocked", (PyCFunction)PyRayletClient_NotifyUnblocked, METH_VARARGS,
     "Notify the local scheduler that we are unblocked."},
    {"compute_put_id", (PyCFunction)PyRayletClient_compute_put_id, METH_VARARGS,
     "Return the object ID for a put call within a task."},
    {"resource_ids", (PyCFunction)PyRayletClient_resource_ids, METH_NOARGS,
     "Get the IDs of the resources that are reserved for this client."},
    {"wait", (PyCFunction)PyRayletClient_Wait, METH_VARARGS,
     "Wait for a list of objects to be created."},
    {"push_error", (PyCFunction)PyRayletClient_PushError, METH_VARARGS,
     "Push an error message to the relevant driver."},
    {"push_profile_events", (PyCFunction)PyRayletClient_PushProfileEvents, METH_VARARGS,
     "Store some profiling events in the GCS."},
    {"free_objects", (PyCFunction)PyRayletClient_FreeObjects, METH_VARARGS,
     "Free a list of objects from object stores."},
    {NULL} /* Sentinel */
};

static PyTypeObject PyRayletClientType = {
    PyVarObject_HEAD_INIT(NULL, 0)      /* ob_size */
    "raylet.RayletClient",              /* tp_name */
    sizeof(PyRayletClient),             /* tp_basicsize */
    0,                                  /* tp_itemsize */
    (destructor)PyRayletClient_dealloc, /* tp_dealloc */
    0,                                  /* tp_print */
    0,                                  /* tp_getattr */
    0,                                  /* tp_setattr */
    0,                                  /* tp_compare */
    0,                                  /* tp_repr */
    0,                                  /* tp_as_number */
    0,                                  /* tp_as_sequence */
    0,                                  /* tp_as_mapping */
    0,                                  /* tp_hash */
    0,                                  /* tp_call */
    0,                                  /* tp_str */
    0,                                  /* tp_getattro */
    0,                                  /* tp_setattro */
    0,                                  /* tp_as_buffer */
    Py_TPFLAGS_DEFAULT,                 /* tp_flags */
    "RayletClient object",              /* tp_doc */
    0,                                  /* tp_traverse */
    0,                                  /* tp_clear */
    0,                                  /* tp_richcompare */
    0,                                  /* tp_weaklistoffset */
    0,                                  /* tp_iter */
    0,                                  /* tp_iternext */
    PyRayletClient_methods,             /* tp_methods */
    0,                                  /* tp_members */
    0,                                  /* tp_getset */
    0,                                  /* tp_base */
    0,                                  /* tp_dict */
    0,                                  /* tp_descr_get */
    0,                                  /* tp_descr_set */
    0,                                  /* tp_dictoffset */
    (initproc)PyRayletClient_init,      /* tp_init */
    0,                                  /* tp_alloc */
    PyType_GenericNew,                  /* tp_new */
};

static PyMethodDef raylet_methods[] = {
    {"check_simple_value", check_simple_value, METH_VARARGS,
     "Should the object be passed by value?"},
    {"compute_task_id", compute_task_id, METH_VARARGS,
     "Return the task ID of an object ID."},
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
    "libraylet",                /* m_name */
    "A module for the raylet.", /* m_doc */
    0,                          /* m_size */
    raylet_methods,             /* m_methods */
    NULL,                       /* m_reload */
    NULL,                       /* m_traverse */
    NULL,                       /* m_clear */
    NULL,                       /* m_free */
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

MOD_INIT(libraylet_library_python) {
  if (PyType_Ready(&PyTaskType) < 0) {
    INITERROR;
  }

  if (PyType_Ready(&PyObjectIDType) < 0) {
    INITERROR;
  }

  if (PyType_Ready(&PyRayletClientType) < 0) {
    INITERROR;
  }

  if (PyType_Ready(&PyRayConfigType) < 0) {
    INITERROR;
  }

#if PY_MAJOR_VERSION >= 3
  PyObject *m = PyModule_Create(&moduledef);
#else
  PyObject *m = Py_InitModule3("libraylet_library_python", raylet_methods,
                               "A module for the raylet.");
#endif

  init_numpy_module();
  init_pickle_module();

  Py_INCREF(&PyTaskType);
  PyModule_AddObject(m, "Task", (PyObject *)&PyTaskType);

  Py_INCREF(&PyObjectIDType);
  PyModule_AddObject(m, "ObjectID", (PyObject *)&PyObjectIDType);

  Py_INCREF(&PyRayletClientType);
  PyModule_AddObject(m, "RayletClient", (PyObject *)&PyRayletClientType);

  char common_error[] = "common.error";
  CommonError = PyErr_NewException(common_error, NULL, NULL);
  Py_INCREF(CommonError);
  PyModule_AddObject(m, "common_error", CommonError);

  Py_INCREF(&PyRayConfigType);
  PyModule_AddObject(m, "RayConfig", (PyObject *)&PyRayConfigType);

  /* Create the global config object. */
  PyObject *config = PyRayConfig_make();
  /* TODO(rkn): Do we need Py_INCREF(config)? */
  PyModule_AddObject(m, "_config", config);

#if PY_MAJOR_VERSION >= 3
  return m;
#endif
}
