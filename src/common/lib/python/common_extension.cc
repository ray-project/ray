#include <Python.h>
#include "bytesobject.h"
#include "node.h"

#include "common.h"
#include "common_extension.h"
#include "task.h"
#include "utarray.h"
#include "utstring.h"

PyObject *CommonError;

/* Initialize pickle module. */

PyObject *pickle_module = NULL;
PyObject *pickle_loads = NULL;
PyObject *pickle_dumps = NULL;
PyObject *pickle_protocol = NULL;

void init_pickle_module(void) {
#if PY_MAJOR_VERSION >= 3
  pickle_module = PyImport_ImportModule("pickle");
#else
  pickle_module = PyImport_ImportModuleNoBlock("cPickle");
#endif
  CHECK(pickle_module != NULL);
  CHECK(PyObject_HasAttrString(pickle_module, "loads"));
  CHECK(PyObject_HasAttrString(pickle_module, "dumps"));
  CHECK(PyObject_HasAttrString(pickle_module, "HIGHEST_PROTOCOL"));
  pickle_loads = PyUnicode_FromString("loads");
  pickle_dumps = PyUnicode_FromString("dumps");
  pickle_protocol = PyObject_GetAttrString(pickle_module, "HIGHEST_PROTOCOL");
  CHECK(pickle_protocol != NULL);
}

TaskBuilder *g_task_builder = NULL;

/* Define the PyObjectID class. */

int PyStringToUniqueID(PyObject *object, ObjectID *object_id) {
  if (PyBytes_Check(object)) {
    memcpy(&object_id->id[0], PyBytes_AsString(object), UNIQUE_ID_SIZE);
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
  memcpy(&self->object_id.id[0], data, sizeof(self->object_id.id));
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
  return PyBytes_FromStringAndSize((char *) task->spec, task->size);
}

static PyObject *PyObjectID_id(PyObject *self) {
  PyObjectID *s = (PyObjectID *) self;
  return PyBytes_FromStringAndSize((char *) &s->object_id.id[0],
                                   sizeof(s->object_id.id));
}

static PyObject *PyObjectID_hex(PyObject *self) {
  PyObjectID *s = (PyObjectID *) self;
  char hex_id[ID_STRING_SIZE];
  ObjectID_to_string(s->object_id, hex_id, ID_STRING_SIZE);
  PyObject *result = PyUnicode_FromString(hex_id);
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
      result = ObjectID_equal(self->object_id, other_id->object_id) ? Py_True
                                                                    : Py_False;
      break;
    case Py_NE:
      result = !ObjectID_equal(self->object_id, other_id->object_id) ? Py_True
                                                                     : Py_False;
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
   * made through UniqueIDHasher in src/common/common.h */
  UniqueIDHasher hash;
  return PyLong_FromSize_t(hash(self->object_id));
}

static long PyObjectID_hash(PyObjectID *self) {
  PyObject *tuple = PyTuple_New(UNIQUE_ID_SIZE);
  for (int i = 0; i < UNIQUE_ID_SIZE; ++i) {
    PyTuple_SetItem(tuple, i, PyLong_FromLong(self->object_id.id[i]));
  }
  long hash = PyObject_Hash(tuple);
  Py_XDECREF(tuple);
  return hash;
}

static PyObject *PyObjectID_repr(PyObjectID *self) {
  char hex_id[ID_STRING_SIZE];
  ObjectID_to_string(self->object_id, hex_id, ID_STRING_SIZE);
  UT_string *repr;
  utstring_new(repr);
  utstring_printf(repr, "ObjectID(%s)", hex_id);
  PyObject *result = PyUnicode_FromString(utstring_body(repr));
  utstring_free(repr);
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

/* Define the PyTask class. */

static int PyTask_init(PyTask *self, PyObject *args, PyObject *kwds) {
  /* ID of the driver that this task originates from. */
  UniqueID driver_id;
  /* ID of the actor this task should run on. */
  UniqueID actor_id = NIL_ACTOR_ID;
  /* How many tasks have been launched on the actor so far? */
  int actor_counter = 0;
  /* True if this is an actor checkpoint task and false otherwise. */
  PyObject *is_actor_checkpoint_method_object = NULL;
  /* ID of the function this task executes. */
  FunctionID function_id;
  /* Arguments of the task (can be PyObjectIDs or Python values). */
  PyObject *arguments;
  /* Number of return values of this task. */
  int num_returns;
  /* The ID of the task that called this task. */
  TaskID parent_task_id;
  /* The number of tasks that the parent task has called prior to this one. */
  int parent_counter;
  /* Resource vector of the required resources to execute this task. */
  PyObject *resource_vector = NULL;
  if (!PyArg_ParseTuple(args, "O&O&OiO&i|O&iOO", &PyObjectToUniqueID,
                        &driver_id, &PyObjectToUniqueID, &function_id,
                        &arguments, &num_returns, &PyObjectToUniqueID,
                        &parent_task_id, &parent_counter, &PyObjectToUniqueID,
                        &actor_id, &actor_counter,
                        &is_actor_checkpoint_method_object, &resource_vector)) {
    return -1;
  }

  bool is_actor_checkpoint_method = false;
  if (is_actor_checkpoint_method_object != NULL &&
      PyObject_IsTrue(is_actor_checkpoint_method_object) == 1) {
    is_actor_checkpoint_method = true;
  }

  Py_ssize_t size = PyList_Size(arguments);
  /* Construct the task specification. */
  TaskSpec_start_construct(
      g_task_builder, driver_id, parent_task_id, parent_counter, actor_id,
      actor_counter, is_actor_checkpoint_method, function_id, num_returns);
  /* Add the task arguments. */
  for (Py_ssize_t i = 0; i < size; ++i) {
    PyObject *arg = PyList_GetItem(arguments, i);
    if (PyObject_IsInstance(arg, (PyObject *) &PyObjectIDType)) {
      TaskSpec_args_add_ref(g_task_builder, ((PyObjectID *) arg)->object_id);
    } else {
      /* We do this check because we cast a signed int to an unsigned int. */
      PyObject *data = PyObject_CallMethodObjArgs(pickle_module, pickle_dumps,
                                                  arg, pickle_protocol, NULL);
      TaskSpec_args_add_val(g_task_builder, (uint8_t *) PyBytes_AS_STRING(data),
                            PyBytes_GET_SIZE(data));
      Py_DECREF(data);
    }
  }
  /* Set the resource vector of the task. */
  if (resource_vector != NULL) {
    CHECK(PyList_Size(resource_vector) == ResourceIndex_MAX);
    for (int i = 0; i < ResourceIndex_MAX; ++i) {
      PyObject *resource_entry = PyList_GetItem(resource_vector, i);
      TaskSpec_set_required_resource(g_task_builder, i,
                                     PyFloat_AsDouble(resource_entry));
    }
  } else {
    for (int i = 0; i < ResourceIndex_MAX; ++i) {
      TaskSpec_set_required_resource(g_task_builder, i,
                                     i == ResourceIndex_CPU ? 1.0 : 0.0);
    }
  }
  /* Compute the task ID and the return object IDs. */
  self->spec = TaskSpec_finish_construct(g_task_builder, &self->size);
  return 0;
}

static void PyTask_dealloc(PyTask *self) {
  if (self->spec != NULL) {
    TaskSpec_free(self->spec);
  }
  Py_TYPE(self)->tp_free((PyObject *) self);
}

static PyObject *PyTask_function_id(PyObject *self) {
  FunctionID function_id = TaskSpec_function(((PyTask *) self)->spec);
  return PyObjectID_make(function_id);
}

static PyObject *PyTask_actor_id(PyObject *self) {
  ActorID actor_id = TaskSpec_actor_id(((PyTask *) self)->spec);
  return PyObjectID_make(actor_id);
}

static PyObject *PyTask_actor_counter(PyObject *self) {
  int64_t actor_counter = TaskSpec_actor_counter(((PyTask *) self)->spec);
  return PyLong_FromLongLong(actor_counter);
}

static PyObject *PyTask_driver_id(PyObject *self) {
  UniqueID driver_id = TaskSpec_driver_id(((PyTask *) self)->spec);
  return PyObjectID_make(driver_id);
}

static PyObject *PyTask_task_id(PyObject *self) {
  TaskID task_id = TaskSpec_task_id(((PyTask *) self)->spec);
  return PyObjectID_make(task_id);
}

static PyObject *PyTask_parent_task_id(PyObject *self) {
  TaskID task_id = TaskSpec_parent_task_id(((PyTask *) self)->spec);
  return PyObjectID_make(task_id);
}

static PyObject *PyTask_parent_counter(PyObject *self) {
  int64_t parent_counter = TaskSpec_parent_counter(((PyTask *) self)->spec);
  return PyLong_FromLongLong(parent_counter);
}

static PyObject *PyTask_arguments(PyObject *self) {
  TaskSpec *task = ((PyTask *) self)->spec;
  int64_t num_args = TaskSpec_num_args(task);
  PyObject *arg_list = PyList_New((Py_ssize_t) num_args);
  for (int i = 0; i < num_args; ++i) {
    if (TaskSpec_arg_by_ref(task, i)) {
      ObjectID object_id = TaskSpec_arg_id(task, i);
      PyList_SetItem(arg_list, i, PyObjectID_make(object_id));
    } else {
      CHECK(pickle_module != NULL);
      CHECK(pickle_loads != NULL);
      PyObject *str =
          PyBytes_FromStringAndSize((char *) TaskSpec_arg_val(task, i),
                                    (Py_ssize_t) TaskSpec_arg_length(task, i));
      PyObject *val =
          PyObject_CallMethodObjArgs(pickle_module, pickle_loads, str, NULL);
      Py_XDECREF(str);
      PyList_SetItem(arg_list, i, val);
    }
  }
  return arg_list;
}

static PyObject *PyTask_required_resources(PyObject *self) {
  TaskSpec *task = ((PyTask *) self)->spec;
  PyObject *required_resources = PyList_New((Py_ssize_t) ResourceIndex_MAX);
  for (int i = 0; i < ResourceIndex_MAX; ++i) {
    double r = TaskSpec_get_required_resource(task, i);
    PyList_SetItem(required_resources, i, PyFloat_FromDouble(r));
  }
  return required_resources;
}

static PyObject *PyTask_returns(PyObject *self) {
  TaskSpec *task = ((PyTask *) self)->spec;
  int64_t num_returns = TaskSpec_num_returns(task);
  PyObject *return_id_list = PyList_New((Py_ssize_t) num_returns);
  for (int i = 0; i < num_returns; ++i) {
    ObjectID object_id = TaskSpec_return(task, i);
    PyList_SetItem(return_id_list, i, PyObjectID_make(object_id));
  }
  return return_id_list;
}

static PyMethodDef PyTask_methods[] = {
    {"function_id", (PyCFunction) PyTask_function_id, METH_NOARGS,
     "Return the function ID for this task."},
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
    {"required_resources", (PyCFunction) PyTask_required_resources, METH_NOARGS,
     "Return the resource vector of the task."},
    {"returns", (PyCFunction) PyTask_returns, METH_NOARGS,
     "Return the object IDs for the return values of the task."},
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
  return (PyObject *) result;
}

/* Define the methods for the module. */

#define SIZE_LIMIT 100
#define NUM_ELEMENTS_LIMIT 1000

#if PY_MAJOR_VERSION >= 3
#define PyInt_Check PyLong_Check
#endif

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
 * @return 0 if the object cannot be serialized in the task and 1 if it can.
 */
int is_simple_value(PyObject *value, int *num_elements_contained) {
  *num_elements_contained += 1;
  if (*num_elements_contained >= NUM_ELEMENTS_LIMIT) {
    return 0;
  }
  if (PyInt_Check(value) || PyLong_Check(value) || value == Py_False ||
      value == Py_True || PyFloat_Check(value) || value == Py_None) {
    return 1;
  }
  if (PyBytes_CheckExact(value)) {
    *num_elements_contained += PyBytes_Size(value);
    return (*num_elements_contained < NUM_ELEMENTS_LIMIT);
  }
  if (PyUnicode_CheckExact(value)) {
    *num_elements_contained += PyUnicode_GET_SIZE(value);
    return (*num_elements_contained < NUM_ELEMENTS_LIMIT);
  }
  if (PyList_CheckExact(value) && PyList_Size(value) < SIZE_LIMIT) {
    for (Py_ssize_t i = 0; i < PyList_Size(value); ++i) {
      if (!is_simple_value(PyList_GetItem(value, i), num_elements_contained)) {
        return 0;
      }
    }
    return (*num_elements_contained < NUM_ELEMENTS_LIMIT);
  }
  if (PyDict_CheckExact(value) && PyDict_Size(value) < SIZE_LIMIT) {
    PyObject *key, *val;
    Py_ssize_t pos = 0;
    while (PyDict_Next(value, &pos, &key, &val)) {
      if (!is_simple_value(key, num_elements_contained) ||
          !is_simple_value(val, num_elements_contained)) {
        return 0;
      }
    }
    return (*num_elements_contained < NUM_ELEMENTS_LIMIT);
  }
  if (PyTuple_CheckExact(value) && PyTuple_Size(value) < SIZE_LIMIT) {
    for (Py_ssize_t i = 0; i < PyTuple_Size(value); ++i) {
      if (!is_simple_value(PyTuple_GetItem(value, i), num_elements_contained)) {
        return 0;
      }
    }
    return (*num_elements_contained < NUM_ELEMENTS_LIMIT);
  }
  return 0;
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
