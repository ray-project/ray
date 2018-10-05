#include <Python.h>
#include "bytesobject.h"
#include "node.h"

// Don't use the deprecated Numpy functions.
#define NPY_NO_DEPRECATED_API NPY_1_7_API_VERSION

#include <numpy/arrayobject.h>

#include "common.h"
#include "common_extension.h"
#include "common_protocol.h"
#include "ray/raylet/task.h"
#include "ray/raylet/task_execution_spec.h"
#include "ray/raylet/task_spec.h"
#include "task.h"

#include <string>

#if PY_MAJOR_VERSION >= 3
#define PyInt_Check PyLong_Check
#endif

PyObject *CommonError;

/* Initialize pickle module. */

PyObject *pickle_module = NULL;
PyObject *pickle_loads = NULL;
PyObject *pickle_dumps = NULL;
PyObject *pickle_protocol = NULL;

int init_numpy_module(void) {
  import_array1(-1);
  return 0;
}

void init_pickle_module(void) {
#if PY_MAJOR_VERSION >= 3
  pickle_module = PyImport_ImportModule("pickle");
#else
  pickle_module = PyImport_ImportModuleNoBlock("cPickle");
#endif
  RAY_CHECK(pickle_module != NULL);
  RAY_CHECK(PyObject_HasAttrString(pickle_module, "loads"));
  RAY_CHECK(PyObject_HasAttrString(pickle_module, "dumps"));
  RAY_CHECK(PyObject_HasAttrString(pickle_module, "HIGHEST_PROTOCOL"));
  pickle_loads = PyUnicode_FromString("loads");
  pickle_dumps = PyUnicode_FromString("dumps");
  pickle_protocol = PyObject_GetAttrString(pickle_module, "HIGHEST_PROTOCOL");
  RAY_CHECK(pickle_protocol != NULL);
}

TaskBuilder *g_task_builder = NULL;

/* Define the PyObjectID class. */

int PyStringToUniqueID(PyObject *object, ObjectID *object_id) {
  if (PyBytes_Check(object)) {
    std::memcpy(object_id->mutable_data(), PyBytes_AsString(object),
                sizeof(*object_id));
    return 1;
  } else {
    PyErr_SetString(PyExc_TypeError, "must be a 20 character string");
    return 0;
  }
}

int PyObjectToUniqueID(PyObject *object, ObjectID *objectid) {
  if (PyObject_IsInstance(object, (PyObject *) &PyObjectIDType)) {
    *objectid = ((PyObjectID *) object)->object_id;
    return 1;
  } else {
    PyErr_SetString(PyExc_TypeError, "must be an ObjectID");
    return 0;
  }
}

int PyListStringToFunctionDescriptor(
    PyObject *object,
    std::vector<std::string> *function_descriptor) {
  if (function_descriptor == nullptr) {
    PyErr_SetString(PyExc_TypeError,
                    "function descriptor must be non-empty pointer");
    return 0;
  }
  function_descriptor->clear();
  std::vector<std::string> string_vector;
  if (PyList_Check(object)) {
    if (PyList_Size(object) == 0) {
      return 1;
    }
    Py_ssize_t size = PyList_Size(object);
    for (Py_ssize_t i = 0; i < size; ++i) {
      PyObject *item = PyList_GetItem(object, i);
      function_descriptor->emplace_back(PyBytes_AsString(item),
                                        PyBytes_Size(item));
    }
    return 1;
  } else {
    PyErr_SetString(PyExc_TypeError, "must be a list of strings");
    return 0;
  }
}

bool use_raylet(PyTask *task) {
  return task->spec == nullptr;
}

static int PyObjectID_init(PyObjectID *self, PyObject *args, PyObject *kwds) {
  const char *data;
  int size;
  if (!PyArg_ParseTuple(args, "s#", &data, &size)) {
    return -1;
  }
  if (size != sizeof(ObjectID)) {
    PyErr_SetString(CommonError,
                    "ObjectID: object id string needs to have length 20");
    return -1;
  }
  std::memcpy(self->object_id.mutable_data(), data, sizeof(self->object_id));
  return 0;
}

/* Create a PyObjectID from C. */
PyObject *PyObjectID_make(ObjectID object_id) {
  PyObjectID *result = PyObject_New(PyObjectID, &PyObjectIDType);
  result = (PyObjectID *) PyObject_Init((PyObject *) result, &PyObjectIDType);
  result->object_id = object_id;
  return (PyObject *) result;
}

/**
 * Convert a string to a Ray task specification Python object.
 *
 * This is called from Python like
 *
 * task = local_scheduler.task_from_string("...")
 *
 * @param task_string String representation of the task specification.
 * @return Python task specification object.
 */
PyObject *PyTask_from_string(PyObject *self, PyObject *args) {
  const char *data;
  int size;
  if (!PyArg_ParseTuple(args, "s#", &data, &size)) {
    return NULL;
  }
  PyTask *result = PyObject_New(PyTask, &PyTaskType);
  result = (PyTask *) PyObject_Init((PyObject *) result, &PyTaskType);
  result->size = size;
  result->spec = TaskSpec_copy((TaskSpec *) data, size);
  /* The created task does not include any execution dependencies. */
  result->execution_dependencies = new std::vector<ObjectID>();
  /* TODO(pcm): Use flatbuffers validation here. */
  return (PyObject *) result;
}

/**
 * Convert a Ray task specification Python object to a string.
 *
 * This is called from Python like
 *
 * s = local_scheduler.task_to_string(task)
 *
 * @param task Ray task specification Python object.
 * @return String representing the task specification.
 */
PyObject *PyTask_to_string(PyObject *self, PyObject *args) {
  PyObject *arg;
  if (!PyArg_ParseTuple(args, "O", &arg)) {
    return NULL;
  }
  PyTask *task = (PyTask *) arg;
  if (!use_raylet(task)) {
    return PyBytes_FromStringAndSize((char *) task->spec, task->size);
  } else {
    flatbuffers::FlatBufferBuilder fbb;
    auto task_spec_string = task->task_spec->ToFlatbuffer(fbb);
    fbb.Finish(task_spec_string);
    return PyBytes_FromStringAndSize((char *) fbb.GetBufferPointer(),
                                     fbb.GetSize());
  }
}

static PyObject *PyObjectID_id(PyObject *self) {
  PyObjectID *s = (PyObjectID *) self;
  return PyBytes_FromStringAndSize((const char *) s->object_id.data(),
                                   sizeof(s->object_id));
}

static PyObject *PyObjectID_hex(PyObject *self) {
  PyObjectID *s = (PyObjectID *) self;
  std::string hex_id = s->object_id.hex();
#if PY_MAJOR_VERSION >= 3
  PyObject *result = PyUnicode_FromStringAndSize(hex_id.data(), hex_id.size());
#else
  PyObject *result = PyBytes_FromStringAndSize(hex_id.data(), hex_id.size());
#endif
  return result;
}

static PyObject *PyObjectID_richcompare(PyObjectID *self,
                                        PyObject *other,
                                        int op) {
  PyObject *result = NULL;
  if (Py_TYPE(self)->tp_richcompare != Py_TYPE(other)->tp_richcompare) {
    result = Py_NotImplemented;
  } else {
    PyObjectID *other_id = (PyObjectID *) other;
    switch (op) {
    case Py_LT:
      result = Py_NotImplemented;
      break;
    case Py_LE:
      result = Py_NotImplemented;
      break;
    case Py_EQ:
      result = self->object_id == other_id->object_id ? Py_True : Py_False;
      break;
    case Py_NE:
      result = !(self->object_id == other_id->object_id) ? Py_True : Py_False;
      break;
    case Py_GT:
      result = Py_NotImplemented;
      break;
    case Py_GE:
      result = Py_NotImplemented;
      break;
    }
  }
  Py_XINCREF(result);
  return result;
}

static PyObject *PyObjectID_redis_shard_hash(PyObjectID *self) {
  /* NOTE: The hash function used here must match the one in get_redis_context
   * in src/common/state/redis.cc. Changes to the hash function should only be
   * made through std::hash in src/common/common.h */
  std::hash<ray::UniqueID> hash;
  return PyLong_FromSize_t(hash(self->object_id));
}

static long PyObjectID_hash(PyObjectID *self) {
  // TODO(pcm): Replace this with a faster hash function. This currently
  // creates a tuple of length 20 and hashes it, which is slow
  PyObject *tuple = PyTuple_New(kUniqueIDSize);
  for (int i = 0; i < kUniqueIDSize; ++i) {
    PyTuple_SetItem(tuple, i, PyLong_FromLong(self->object_id.data()[i]));
  }
  long hash = PyObject_Hash(tuple);
  Py_XDECREF(tuple);
  return hash;
}

static PyObject *PyObjectID_repr(PyObjectID *self) {
  std::string repr = "ObjectID(" + self->object_id.hex() + ")";
  PyObject *result = PyUnicode_FromString(repr.c_str());
  return result;
}

static PyObject *PyObjectID___reduce__(PyObjectID *self) {
  PyErr_SetString(CommonError, "ObjectID objects cannot be serialized.");
  return NULL;
}

static PyMethodDef PyObjectID_methods[] = {
    {"id", (PyCFunction) PyObjectID_id, METH_NOARGS,
     "Return the hash associated with this ObjectID"},
    {"redis_shard_hash", (PyCFunction) PyObjectID_redis_shard_hash, METH_NOARGS,
     "Return the redis shard that this ObjectID is associated with"},
    {"hex", (PyCFunction) PyObjectID_hex, METH_NOARGS,
     "Return the object ID as a string in hex."},
    {"__reduce__", (PyCFunction) PyObjectID___reduce__, METH_NOARGS,
     "Say how to pickle this ObjectID. This raises an exception to prevent"
     "object IDs from being serialized."},
    {NULL} /* Sentinel */
};

static PyMemberDef PyObjectID_members[] = {
    {NULL} /* Sentinel */
};

PyTypeObject PyObjectIDType = {
    PyVarObject_HEAD_INIT(NULL, 0)        /* ob_size */
    "common.ObjectID",                    /* tp_name */
    sizeof(PyObjectID),                   /* tp_basicsize */
    0,                                    /* tp_itemsize */
    0,                                    /* tp_dealloc */
    0,                                    /* tp_print */
    0,                                    /* tp_getattr */
    0,                                    /* tp_setattr */
    0,                                    /* tp_compare */
    (reprfunc) PyObjectID_repr,           /* tp_repr */
    0,                                    /* tp_as_number */
    0,                                    /* tp_as_sequence */
    0,                                    /* tp_as_mapping */
    (hashfunc) PyObjectID_hash,           /* tp_hash */
    0,                                    /* tp_call */
    0,                                    /* tp_str */
    0,                                    /* tp_getattro */
    0,                                    /* tp_setattro */
    0,                                    /* tp_as_buffer */
    Py_TPFLAGS_DEFAULT,                   /* tp_flags */
    "ObjectID object",                    /* tp_doc */
    0,                                    /* tp_traverse */
    0,                                    /* tp_clear */
    (richcmpfunc) PyObjectID_richcompare, /* tp_richcompare */
    0,                                    /* tp_weaklistoffset */
    0,                                    /* tp_iter */
    0,                                    /* tp_iternext */
    PyObjectID_methods,                   /* tp_methods */
    PyObjectID_members,                   /* tp_members */
    0,                                    /* tp_getset */
    0,                                    /* tp_base */
    0,                                    /* tp_dict */
    0,                                    /* tp_descr_get */
    0,                                    /* tp_descr_set */
    0,                                    /* tp_dictoffset */
    (initproc) PyObjectID_init,           /* tp_init */
    0,                                    /* tp_alloc */
    PyType_GenericNew,                    /* tp_new */
};

// Define the PyTask class.

int resource_map_from_python_dict(
    PyObject *resource_map,
    std::unordered_map<std::string, double> &out) {
  RAY_CHECK(out.size() == 0);

  PyObject *key, *value;
  Py_ssize_t position = 0;
  if (!PyDict_Check(resource_map)) {
    PyErr_SetString(PyExc_TypeError, "resource_map must be a dictionary");
    return -1;
  }

  while (PyDict_Next(resource_map, &position, &key, &value)) {
#if PY_MAJOR_VERSION >= 3
    if (!PyUnicode_Check(key)) {
      PyErr_SetString(PyExc_TypeError,
                      "the keys in resource_map must be strings");
      return -1;
    }
#else
    if (!PyBytes_Check(key)) {
      PyErr_SetString(PyExc_TypeError,
                      "the keys in resource_map must be strings");
      return -1;
    }
#endif

    // Check that the resource quantities are numbers.
    if (!(PyFloat_Check(value) || PyInt_Check(value) || PyLong_Check(value))) {
      PyErr_SetString(PyExc_TypeError,
                      "the values in resource_map must be floats");
      return -1;
    }
    // Handle the case where the key is a bytes object and the case where it
    // is a unicode object.
    std::string resource_name;
    if (PyUnicode_Check(key)) {
      PyObject *ascii_key = PyUnicode_AsASCIIString(key);
      resource_name =
          std::string(PyBytes_AsString(ascii_key), PyBytes_Size(ascii_key));
      Py_DECREF(ascii_key);
    } else {
      resource_name = std::string(PyBytes_AsString(key), PyBytes_Size(key));
    }
    out[resource_name] = PyFloat_AsDouble(value);
  }
  return 0;
}

static int PyTask_init(PyTask *self, PyObject *args, PyObject *kwds) {
  // ID of the driver that this task originates from.
  UniqueID driver_id;
  // ID of the actor this task should run on.
  UniqueID actor_id = ActorID::nil();
  // ID of the actor handle used to submit this task.
  UniqueID actor_handle_id = ActorHandleID::nil();
  // How many tasks have been launched on the actor so far?
  int actor_counter = 0;
  // True if this is an actor checkpoint task and false otherwise.
  PyObject *is_actor_checkpoint_method_object = nullptr;
  // Arguments of the task (can be PyObjectIDs or Python values).
  PyObject *arguments;
  // Number of return values of this task.
  int num_returns;
  // The ID of the task that called this task.
  TaskID parent_task_id;
  // The number of tasks that the parent task has called prior to this one.
  int parent_counter;
  // The actor creation ID.
  ActorID actor_creation_id = ActorID::nil();
  // The dummy object for the actor creation task (if this is an actor method).
  ObjectID actor_creation_dummy_object_id = ObjectID::nil();
  // Arguments of the task that are execution-dependent. These must be
  // PyObjectIDs).
  PyObject *execution_arguments = nullptr;
  // Dictionary of resource requirements for this task.
  PyObject *resource_map = nullptr;
  // Dictionary of required placement resources for this task.
  PyObject *placement_resource_map = nullptr;
  // True if we should use the raylet code path and false otherwise.
  PyObject *use_raylet_object = nullptr;
  // Function descriptor.
  std::vector<std::string> function_descriptor;
  if (!PyArg_ParseTuple(
          args, "O&O&OiO&i|O&O&O&O&iOOOOO", &PyObjectToUniqueID, &driver_id,
          &PyListStringToFunctionDescriptor, &function_descriptor, &arguments,
          &num_returns, &PyObjectToUniqueID, &parent_task_id, &parent_counter,
          &PyObjectToUniqueID, &actor_creation_id, &PyObjectToUniqueID,
          &actor_creation_dummy_object_id, &PyObjectToUniqueID, &actor_id,
          &PyObjectToUniqueID, &actor_handle_id, &actor_counter,
          &is_actor_checkpoint_method_object, &execution_arguments,
          &resource_map, &placement_resource_map, &use_raylet_object)) {
    return -1;
  }

  bool is_actor_checkpoint_method = false;
  if (is_actor_checkpoint_method_object != nullptr &&
      PyObject_IsTrue(is_actor_checkpoint_method_object) == 1) {
    is_actor_checkpoint_method = true;
  }

  // Parse the resource map.
  std::unordered_map<std::string, double> required_resources;
  std::unordered_map<std::string, double> required_placement_resources;

  if (resource_map != nullptr) {
    if (resource_map_from_python_dict(resource_map, required_resources) != 0) {
      return -1;
    }
  }

  if (required_resources.count("CPU") == 0) {
    required_resources["CPU"] = 1.0;
  }

  if (placement_resource_map != nullptr) {
    if (resource_map_from_python_dict(placement_resource_map,
                                      required_placement_resources) != 0) {
      return -1;
    }
  }

  Py_ssize_t num_args = PyList_Size(arguments);

  bool use_raylet = false;
  if (use_raylet_object != nullptr && PyObject_IsTrue(use_raylet_object) == 1) {
    use_raylet = true;
  }
  self->spec = nullptr;
  self->task_spec = nullptr;

  // Create the task spec.
  if (!use_raylet) {
    // The non-raylet code path.

    // Construct the task specification.
    TaskSpec_start_construct(
        g_task_builder, driver_id, parent_task_id, parent_counter,
        actor_creation_id, actor_creation_dummy_object_id, actor_id,
        actor_handle_id, actor_counter, is_actor_checkpoint_method,
        function_descriptor, num_returns);
    // Add the task arguments.
    for (Py_ssize_t i = 0; i < num_args; ++i) {
      PyObject *arg = PyList_GetItem(arguments, i);
      if (PyObject_IsInstance(arg,
                              reinterpret_cast<PyObject *>(&PyObjectIDType))) {
        TaskSpec_args_add_ref(g_task_builder,
                              &(reinterpret_cast<PyObjectID *>(arg))->object_id,
                              1);
      } else {
        PyObject *data = PyObject_CallMethodObjArgs(pickle_module, pickle_dumps,
                                                    arg, pickle_protocol, NULL);
        TaskSpec_args_add_val(
            g_task_builder, reinterpret_cast<uint8_t *>(PyBytes_AsString(data)),
            PyBytes_Size(data));
        Py_DECREF(data);
      }
    }
    // Set the resource requirements for the task.
    for (auto const &resource_pair : required_resources) {
      TaskSpec_set_required_resource(g_task_builder, resource_pair.first,
                                     resource_pair.second);
    }

    // Compute the task ID and the return object IDs.
    self->spec = TaskSpec_finish_construct(g_task_builder, &self->size);

  } else {
    // The raylet code path.

    // Parse the arguments from the list.
    std::vector<std::shared_ptr<ray::raylet::TaskArgument>> args;
    for (Py_ssize_t i = 0; i < num_args; ++i) {
      PyObject *arg = PyList_GetItem(arguments, i);
      if (PyObject_IsInstance(arg,
                              reinterpret_cast<PyObject *>(&PyObjectIDType))) {
        std::vector<ObjectID> references = {
            reinterpret_cast<PyObjectID *>(arg)->object_id};
        args.push_back(
            std::make_shared<ray::raylet::TaskArgumentByReference>(references));
      } else {
        PyObject *data = PyObject_CallMethodObjArgs(pickle_module, pickle_dumps,
                                                    arg, pickle_protocol, NULL);
        args.push_back(std::make_shared<ray::raylet::TaskArgumentByValue>(
            reinterpret_cast<uint8_t *>(PyBytes_AsString(data)),
            PyBytes_Size(data)));
        Py_DECREF(data);
      }
    }

    self->task_spec = new ray::raylet::TaskSpecification(
        driver_id, parent_task_id, parent_counter, actor_creation_id,
        actor_creation_dummy_object_id, actor_id, actor_handle_id,
        actor_counter, args, num_returns, required_resources,
        required_placement_resources, Language::PYTHON, function_descriptor);
  }

  /* Set the task's execution dependencies. */
  self->execution_dependencies = new std::vector<ObjectID>();
  if (execution_arguments != NULL) {
    Py_ssize_t num_execution_args = PyList_Size(execution_arguments);
    for (Py_ssize_t i = 0; i < num_execution_args; ++i) {
      PyObject *execution_arg = PyList_GetItem(execution_arguments, i);
      if (!PyObject_IsInstance(execution_arg, (PyObject *) &PyObjectIDType)) {
        PyErr_SetString(PyExc_TypeError,
                        "Execution arguments must be an ObjectID.");
        return -1;
      }
      self->execution_dependencies->push_back(
          ((PyObjectID *) execution_arg)->object_id);
    }
  }

  return 0;
}

static void PyTask_dealloc(PyTask *self) {
  if (!use_raylet(self)) {
    TaskSpec_free(self->spec);
  } else {
    delete self->task_spec;
  }
  delete self->execution_dependencies;
  Py_TYPE(self)->tp_free(reinterpret_cast<PyObject *>(self));
}

// Helper function to change a function descriptr to Python list.
static PyObject *VectorStringToPyBytesList(
    const std::vector<std::string> &function_descriptor) {
  size_t size = function_descriptor.size();
  PyObject *return_list = PyList_New(static_cast<Py_ssize_t>(size));
  for (size_t i = 0; i < size; ++i) {
    auto py_bytes = PyBytes_FromStringAndSize(function_descriptor[i].data(),
                                              function_descriptor[i].size());
    PyList_SetItem(return_list, i, py_bytes);
  }
  return return_list;
}

static PyObject *PyTask_function_descriptor_vector(PyTask *self) {
  std::vector<std::string> function_descriptor;
  if (!use_raylet(self)) {
    function_descriptor = TaskSpec_function_descriptor(self->spec);
  } else {
    function_descriptor = self->task_spec->FunctionDescriptor();
  }
  return VectorStringToPyBytesList(function_descriptor);
}

static PyObject *PyTask_actor_id(PyTask *self) {
  ActorID actor_id;
  if (!use_raylet(self)) {
    actor_id = TaskSpec_actor_id(self->spec);
  } else {
    actor_id = self->task_spec->ActorId();
  }
  return PyObjectID_make(actor_id);
}

static PyObject *PyTask_actor_counter(PyTask *self) {
  int64_t actor_counter;
  if (!use_raylet(self)) {
    actor_counter = TaskSpec_actor_counter(self->spec);
  } else {
    actor_counter = self->task_spec->ActorCounter();
  }
  return PyLong_FromLongLong(actor_counter);
}

static PyObject *PyTask_driver_id(PyTask *self) {
  UniqueID driver_id;
  if (!use_raylet(self)) {
    driver_id = TaskSpec_driver_id(self->spec);
  } else {
    driver_id = self->task_spec->DriverId();
  }
  return PyObjectID_make(driver_id);
}

static PyObject *PyTask_task_id(PyTask *self) {
  TaskID task_id;
  if (!use_raylet(self)) {
    task_id = TaskSpec_task_id(self->spec);
  } else {
    task_id = self->task_spec->TaskId();
  }
  return PyObjectID_make(task_id);
}

static PyObject *PyTask_parent_task_id(PyTask *self) {
  TaskID task_id;
  if (!use_raylet(self)) {
    task_id = TaskSpec_parent_task_id(self->spec);
  } else {
    task_id = self->task_spec->ParentTaskId();
  }
  return PyObjectID_make(task_id);
}

static PyObject *PyTask_parent_counter(PyTask *self) {
  int64_t parent_counter;
  if (!use_raylet(self)) {
    parent_counter = TaskSpec_parent_counter(self->spec);
  } else {
    parent_counter = self->task_spec->ParentCounter();
  }
  return PyLong_FromLongLong(parent_counter);
}

static PyObject *PyTask_arguments(PyTask *self) {
  TaskSpec *task = self->spec;
  ray::raylet::TaskSpecification *task_spec = self->task_spec;

  int64_t num_args;
  if (!use_raylet(self)) {
    num_args = TaskSpec_num_args(task);
  } else {
    num_args = self->task_spec->NumArgs();
  }

  PyObject *arg_list = PyList_New((Py_ssize_t) num_args);
  for (int i = 0; i < num_args; ++i) {
    int count;
    if (!use_raylet(self)) {
      count = TaskSpec_arg_id_count(task, i);
    } else {
      count = task_spec->ArgIdCount(i);
    }

    if (count > 0) {
      assert(count == 1);

      ObjectID object_id;
      if (!use_raylet(self)) {
        object_id = TaskSpec_arg_id(task, i, 0);
      } else {
        object_id = task_spec->ArgId(i, 0);
      }

      PyList_SetItem(arg_list, i, PyObjectID_make(object_id));
    } else {
      RAY_CHECK(pickle_module != NULL);
      RAY_CHECK(pickle_loads != NULL);

      const uint8_t *arg_val;
      int64_t arg_length;
      if (!use_raylet(self)) {
        arg_val = TaskSpec_arg_val(task, i);
        arg_length = TaskSpec_arg_length(task, i);
      } else {
        arg_val = task_spec->ArgVal(i);
        arg_length = task_spec->ArgValLength(i);
      }

      PyObject *str =
          PyBytes_FromStringAndSize(reinterpret_cast<const char *>(arg_val),
                                    static_cast<Py_ssize_t>(arg_length));
      PyObject *val =
          PyObject_CallMethodObjArgs(pickle_module, pickle_loads, str, NULL);
      Py_XDECREF(str);
      PyList_SetItem(arg_list, i, val);
    }
  }
  return arg_list;
}

static PyObject *PyTask_actor_creation_id(PyTask *self) {
  ActorID actor_creation_id;
  if (!use_raylet(self)) {
    actor_creation_id = TaskSpec_actor_creation_id(self->spec);
  } else {
    actor_creation_id = self->task_spec->ActorCreationId();
  }
  return PyObjectID_make(actor_creation_id);
}

static PyObject *PyTask_actor_creation_dummy_object_id(PyTask *self) {
  ObjectID actor_creation_dummy_object_id;
  if (!use_raylet(self)) {
    if (TaskSpec_is_actor_task(self->spec)) {
      actor_creation_dummy_object_id =
          TaskSpec_actor_creation_dummy_object_id(self->spec);
    } else {
      actor_creation_dummy_object_id = ObjectID::nil();
    }
  } else {
    actor_creation_dummy_object_id =
        self->task_spec->ActorCreationDummyObjectId();
  }
  return PyObjectID_make(actor_creation_dummy_object_id);
}

static PyObject *PyTask_required_resources(PyTask *self) {
  PyObject *required_resources = PyDict_New();

  std::unordered_map<std::string, double> resource_map;
  if (!use_raylet(self)) {
    resource_map = TaskSpec_get_required_resources(self->spec);
  } else {
    resource_map = self->task_spec->GetRequiredResources().GetResourceMap();
  }

  for (auto const &resource_pair : resource_map) {
    std::string resource_name = resource_pair.first;
#if PY_MAJOR_VERSION >= 3
    PyObject *key =
        PyUnicode_FromStringAndSize(resource_name.data(), resource_name.size());
#else
    PyObject *key =
        PyBytes_FromStringAndSize(resource_name.data(), resource_name.size());
#endif
    PyObject *value = PyFloat_FromDouble(resource_pair.second);
    PyDict_SetItem(required_resources, key, value);
    Py_DECREF(key);
    Py_DECREF(value);
  }
  return required_resources;
}

static PyObject *PyTask_returns(PyTask *self) {
  TaskSpec *task = self->spec;
  ray::raylet::TaskSpecification *task_spec = self->task_spec;

  int64_t num_returns;
  if (!use_raylet(self)) {
    num_returns = TaskSpec_num_returns(task);
  } else {
    num_returns = task_spec->NumReturns();
  }

  PyObject *return_id_list = PyList_New((Py_ssize_t) num_returns);
  for (int i = 0; i < num_returns; ++i) {
    ObjectID object_id;
    if (!use_raylet(self)) {
      object_id = TaskSpec_return(task, i);
    } else {
      object_id = task_spec->ReturnId(i);
    }
    PyList_SetItem(return_id_list, i, PyObjectID_make(object_id));
  }
  return return_id_list;
}

static PyObject *PyTask_execution_dependencies_string(PyTask *self) {
  flatbuffers::FlatBufferBuilder fbb;
  auto execution_dependencies = CreateTaskExecutionDependencies(
      fbb, to_flatbuf(fbb, *self->execution_dependencies));
  fbb.Finish(execution_dependencies);
  return PyBytes_FromStringAndSize((char *) fbb.GetBufferPointer(),
                                   fbb.GetSize());
}

static PyObject *PyTask_to_serialized_flatbuf(PyTask *self) {
  RAY_CHECK(use_raylet(self));

  const std::vector<ObjectID> execution_dependencies(
      *self->execution_dependencies);
  auto const execution_spec = ray::raylet::TaskExecutionSpecification(
      std::move(execution_dependencies));
  auto const task = ray::raylet::Task(execution_spec, *self->task_spec);

  flatbuffers::FlatBufferBuilder fbb;
  auto task_flatbuffer = task.ToFlatbuffer(fbb);
  fbb.Finish(task_flatbuffer);

  return PyBytes_FromStringAndSize(
      reinterpret_cast<char *>(fbb.GetBufferPointer()), fbb.GetSize());
}

static PyMethodDef PyTask_methods[] = {
    {"function_descriptor_list",
     (PyCFunction) PyTask_function_descriptor_vector, METH_NOARGS,
     "Return the function descriptor for this task."},
    {"parent_task_id", (PyCFunction) PyTask_parent_task_id, METH_NOARGS,
     "Return the task ID of the parent task."},
    {"parent_counter", (PyCFunction) PyTask_parent_counter, METH_NOARGS,
     "Return the parent counter of this task."},
    {"actor_id", (PyCFunction) PyTask_actor_id, METH_NOARGS,
     "Return the actor ID for this task."},
    {"actor_counter", (PyCFunction) PyTask_actor_counter, METH_NOARGS,
     "Return the actor counter for this task."},
    {"driver_id", (PyCFunction) PyTask_driver_id, METH_NOARGS,
     "Return the driver ID for this task."},
    {"task_id", (PyCFunction) PyTask_task_id, METH_NOARGS,
     "Return the task ID for this task."},
    {"arguments", (PyCFunction) PyTask_arguments, METH_NOARGS,
     "Return the arguments for the task."},
    {"actor_creation_id", (PyCFunction) PyTask_actor_creation_id, METH_NOARGS,
     "Return the actor creation ID for the task."},
    {"actor_creation_dummy_object_id",
     (PyCFunction) PyTask_actor_creation_dummy_object_id, METH_NOARGS,
     "Return the actor creation dummy object ID for the task."},
    {"required_resources", (PyCFunction) PyTask_required_resources, METH_NOARGS,
     "Return the resource vector of the task."},
    {"returns", (PyCFunction) PyTask_returns, METH_NOARGS,
     "Return the object IDs for the return values of the task."},
    {"execution_dependencies_string",
     (PyCFunction) PyTask_execution_dependencies_string, METH_NOARGS,
     "Return the execution dependencies for the task as a string."},
    {"_serialized_raylet_task", (PyCFunction) PyTask_to_serialized_flatbuf,
     METH_NOARGS,
     "This is a hack used to create a serialized flatbuffer object for the "
     "driver task. We're doing this because creating the flatbuffer object in "
     "Python didn't seem to work."},
    {NULL} /* Sentinel */
};

PyTypeObject PyTaskType = {
    PyVarObject_HEAD_INIT(NULL, 0) /* ob_size */
    "task.Task",                   /* tp_name */
    sizeof(PyTask),                /* tp_basicsize */
    0,                             /* tp_itemsize */
    (destructor) PyTask_dealloc,   /* tp_dealloc */
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
    "Task object",                 /* tp_doc */
    0,                             /* tp_traverse */
    0,                             /* tp_clear */
    0,                             /* tp_richcompare */
    0,                             /* tp_weaklistoffset */
    0,                             /* tp_iter */
    0,                             /* tp_iternext */
    PyTask_methods,                /* tp_methods */
    0,                             /* tp_members */
    0,                             /* tp_getset */
    0,                             /* tp_base */
    0,                             /* tp_dict */
    0,                             /* tp_descr_get */
    0,                             /* tp_descr_set */
    0,                             /* tp_dictoffset */
    (initproc) PyTask_init,        /* tp_init */
    0,                             /* tp_alloc */
    PyType_GenericNew,             /* tp_new */
};

/* Create a PyTask from a C struct. The resulting PyTask takes ownership of the
 * TaskSpec and will deallocate the TaskSpec in the PyTask destructor. */
PyObject *PyTask_make(TaskSpec *task_spec, int64_t task_size) {
  PyTask *result = PyObject_New(PyTask, &PyTaskType);
  result = (PyTask *) PyObject_Init((PyObject *) result, &PyTaskType);
  result->spec = task_spec;
  result->size = task_size;
  /* The created task does not include any execution dependencies. */
  result->execution_dependencies = new std::vector<ObjectID>();
  return (PyObject *) result;
}

/* Define the methods for the module. */

/**
 * This method checks if a Python object is sufficiently simple that it can be
 * serialized and passed by value as an argument to a task (without being put in
 * the object store). The details of which objects are sufficiently simple are
 * defined by this method and are not particularly important. But for
 * performance reasons, it is better to place "small" objects in the task itself
 * and "large" objects in the object store.
 *
 * @param value The Python object in question.
 * @param num_elements_contained If this method returns 1, then the number of
 *        objects recursively contained within this object will be added to the
 *        value at this address. This is used to make sure that we do not
 *        serialize objects that are too large.
 * @return False if the object cannot be serialized in the task and true if it
 *         can.
 */
bool is_simple_value(PyObject *value, int *num_elements_contained) {
  *num_elements_contained += 1;
  if (*num_elements_contained >= RayConfig::instance().num_elements_limit()) {
    return false;
  }
  if (PyInt_Check(value) || PyLong_Check(value) || value == Py_False ||
      value == Py_True || PyFloat_Check(value) || value == Py_None) {
    return true;
  }
  if (PyBytes_CheckExact(value)) {
    *num_elements_contained += PyBytes_Size(value);
    return (*num_elements_contained <
            RayConfig::instance().num_elements_limit());
  }
  if (PyUnicode_CheckExact(value)) {
    *num_elements_contained += PyUnicode_GET_SIZE(value);
    return (*num_elements_contained <
            RayConfig::instance().num_elements_limit());
  }
  if (PyList_CheckExact(value) &&
      PyList_Size(value) < RayConfig::instance().size_limit()) {
    for (Py_ssize_t i = 0; i < PyList_Size(value); ++i) {
      if (!is_simple_value(PyList_GetItem(value, i), num_elements_contained)) {
        return false;
      }
    }
    return (*num_elements_contained <
            RayConfig::instance().num_elements_limit());
  }
  if (PyDict_CheckExact(value) &&
      PyDict_Size(value) < RayConfig::instance().size_limit()) {
    PyObject *key, *val;
    Py_ssize_t pos = 0;
    while (PyDict_Next(value, &pos, &key, &val)) {
      if (!is_simple_value(key, num_elements_contained) ||
          !is_simple_value(val, num_elements_contained)) {
        return false;
      }
    }
    return (*num_elements_contained <
            RayConfig::instance().num_elements_limit());
  }
  if (PyTuple_CheckExact(value) &&
      PyTuple_Size(value) < RayConfig::instance().size_limit()) {
    for (Py_ssize_t i = 0; i < PyTuple_Size(value); ++i) {
      if (!is_simple_value(PyTuple_GetItem(value, i), num_elements_contained)) {
        return false;
      }
    }
    return (*num_elements_contained <
            RayConfig::instance().num_elements_limit());
  }
  if (PyArray_CheckExact(value)) {
    PyArrayObject *array = reinterpret_cast<PyArrayObject *>(value);
    if (PyArray_TYPE(array) == NPY_OBJECT) {
      return false;
    }
    *num_elements_contained += PyArray_NBYTES(array);
    return (*num_elements_contained <
            RayConfig::instance().num_elements_limit());
  }
  return false;
}

PyObject *check_simple_value(PyObject *self, PyObject *args) {
  PyObject *value;
  if (!PyArg_ParseTuple(args, "O", &value)) {
    return NULL;
  }
  int num_elements_contained = 0;
  if (is_simple_value(value, &num_elements_contained)) {
    Py_RETURN_TRUE;
  }
  Py_RETURN_FALSE;
}

PyObject *compute_task_id(PyObject *self, PyObject *args) {
  ObjectID object_id;
  if (!PyArg_ParseTuple(args, "O&", &PyObjectToUniqueID, &object_id)) {
    return NULL;
  }
  TaskID task_id = ray::ComputeTaskId(object_id);
  return PyObjectID_make(task_id);
}
