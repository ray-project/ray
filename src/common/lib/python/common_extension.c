#include <Python.h>
#include "node.h"

#include "common_extension.h"
#include "task.h"
#include "utarray.h"
#include "utstring.h"

PyObject *CommonError;

#define MARSHAL_VERSION 2

/* Define the PyObjectID class. */

int PyObjectToUniqueID(PyObject *object, object_id *objectid) {
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
  if (size != UNIQUE_ID_SIZE) {
    PyErr_SetString(CommonError,
                    "ObjectID: object id string needs to have length 20");
    return -1;
  }
  memcpy(&self->object_id.id[0], data, UNIQUE_ID_SIZE);
  return 0;
}

/* Create a PyObjectID from C. */
PyObject *PyObjectID_make(object_id object_id) {
  PyObjectID *result = PyObject_New(PyObjectID, &PyObjectIDType);
  result = (PyObjectID *) PyObject_Init((PyObject *) result, &PyObjectIDType);
  result->object_id = object_id;
  return (PyObject *) result;
}

static PyObject *PyObjectID_id(PyObject *self) {
  PyObjectID *s = (PyObjectID *) self;
  return PyString_FromStringAndSize((char *) &s->object_id.id[0],
                                    UNIQUE_ID_SIZE);
}

static PyObject *PyObjectID___reduce__(PyObjectID *self) {
  PyErr_SetString(CommonError, "ObjectID objects cannot be serialized.");
  return NULL;
}

static PyMethodDef PyObjectID_methods[] = {
    {"id", (PyCFunction) PyObjectID_id, METH_NOARGS,
     "Return the hash associated with this ObjectID"},
    {"__reduce__", (PyCFunction) PyObjectID___reduce__, METH_NOARGS,
     "Say how to pickle this ObjectID. This raises an exception to prevent"
     "object IDs from being serialized."},
    {NULL} /* Sentinel */
};

static PyMemberDef PyObjectID_members[] = {
    {NULL} /* Sentinel */
};

PyTypeObject PyObjectIDType = {
    PyObject_HEAD_INIT(NULL) 0, /* ob_size */
    "common.ObjectID",          /* tp_name */
    sizeof(PyObjectID),         /* tp_basicsize */
    0,                          /* tp_itemsize */
    0,                          /* tp_dealloc */
    0,                          /* tp_print */
    0,                          /* tp_getattr */
    0,                          /* tp_setattr */
    0,                          /* tp_compare */
    0,                          /* tp_repr */
    0,                          /* tp_as_number */
    0,                          /* tp_as_sequence */
    0,                          /* tp_as_mapping */
    0,                          /* tp_hash */
    0,                          /* tp_call */
    0,                          /* tp_str */
    0,                          /* tp_getattro */
    0,                          /* tp_setattro */
    0,                          /* tp_as_buffer */
    Py_TPFLAGS_DEFAULT,         /* tp_flags */
    "ObjectID object",          /* tp_doc */
    0,                          /* tp_traverse */
    0,                          /* tp_clear */
    0,                          /* tp_richcompare */
    0,                          /* tp_weaklistoffset */
    0,                          /* tp_iter */
    0,                          /* tp_iternext */
    PyObjectID_methods,         /* tp_methods */
    PyObjectID_members,         /* tp_members */
    0,                          /* tp_getset */
    0,                          /* tp_base */
    0,                          /* tp_dict */
    0,                          /* tp_descr_get */
    0,                          /* tp_descr_set */
    0,                          /* tp_dictoffset */
    (initproc) PyObjectID_init, /* tp_init */
    0,                          /* tp_alloc */
    PyType_GenericNew,          /* tp_new */
};

/* Define the PyTask class. */

static int PyTask_init(PyTask *self, PyObject *args, PyObject *kwds) {
  function_id function_id;
  /* Arguments of the task (can be PyObjectIDs or Python values). */
  PyObject *arguments;
  /* Array of pointers to string representations of pass-by-value args. */
  UT_array *val_repr_ptrs;
  utarray_new(val_repr_ptrs, &ut_ptr_icd);
  int num_returns;
  if (!PyArg_ParseTuple(args, "O&Oi", &PyObjectToUniqueID, &function_id,
                        &arguments, &num_returns)) {
    return -1;
  }
  size_t size = PyList_Size(arguments);
  /* Determine the size of pass by value data in bytes. */
  size_t value_data_bytes = 0;
  for (size_t i = 0; i < size; ++i) {
    PyObject *arg = PyList_GetItem(arguments, i);
    if (!PyObject_IsInstance(arg, (PyObject *) &PyObjectIDType)) {
      PyObject *data = PyMarshal_WriteObjectToString(arg, MARSHAL_VERSION);
      value_data_bytes += PyString_Size(data);
      utarray_push_back(val_repr_ptrs, &data);
    }
  }
  /* Construct the task specification. */
  int val_repr_index = 0;
  self->spec =
      alloc_task_spec(function_id, size, num_returns, value_data_bytes);
  /* Add the task arguments. */
  for (size_t i = 0; i < size; ++i) {
    PyObject *arg = PyList_GetItem(arguments, i);
    if (PyObject_IsInstance(arg, (PyObject *) &PyObjectIDType)) {
      task_args_add_ref(self->spec, ((PyObjectID *) arg)->object_id);
    } else {
      PyObject *data =
          *((PyObject **) utarray_eltptr(val_repr_ptrs, val_repr_index));
      task_args_add_val(self->spec, (uint8_t *) PyString_AS_STRING(data),
                        PyString_GET_SIZE(data));
      Py_DECREF(data);
      val_repr_index += 1;
    }
  }
  utarray_free(val_repr_ptrs);
  /* Generate and add the object IDs for the return values. */
  for (size_t i = 0; i < num_returns; ++i) {
    /* TODO(rkn): Later, this should be computed as a deterministic hash of (1)
     * the contents of the task, (2) the index i, and (3) a counter of the
     * number of tasks launched so far by the parent task. For now, we generate
     * it randomly. */
    *task_return(self->spec, i) = globally_unique_id();
  }
  return 0;
}

static void PyTask_dealloc(PyTask *self) {
  free_task_spec(self->spec);
  Py_TYPE(self)->tp_free((PyObject *) self);
}

static PyObject *PyTask_function_id(PyObject *self) {
  function_id function_id = *task_function(((PyTask *) self)->spec);
  return PyObjectID_make(function_id);
}

static PyObject *PyTask_arguments(PyObject *self) {
  int64_t num_args = task_num_args(((PyTask *) self)->spec);
  PyObject *arg_list = PyList_New((Py_ssize_t) num_args);
  task_spec *task = ((PyTask *) self)->spec;
  for (int i = 0; i < num_args; ++i) {
    if (task_arg_type(task, i) == ARG_BY_REF) {
      object_id object_id = *task_arg_id(task, i);
      PyList_SetItem(arg_list, i, PyObjectID_make(object_id));
    } else {
      PyObject *s =
          PyMarshal_ReadObjectFromString((char *) task_arg_val(task, i),
                                         (Py_ssize_t) task_arg_length(task, i));
      PyList_SetItem(arg_list, i, s);
    }
  }
  return arg_list;
}

static PyObject *PyTask_returns(PyObject *self) {
  int64_t num_returns = task_num_returns(((PyTask *) self)->spec);
  PyObject *return_id_list = PyList_New((Py_ssize_t) num_returns);
  task_spec *task = ((PyTask *) self)->spec;
  for (int i = 0; i < num_returns; ++i) {
    object_id object_id = *task_return(task, i);
    PyList_SetItem(return_id_list, i, PyObjectID_make(object_id));
  }
  return return_id_list;
}

static PyMethodDef PyTask_methods[] = {
    {"function_id", (PyCFunction) PyTask_function_id, METH_NOARGS,
     "Return the function ID for this task."},
    {"arguments", (PyCFunction) PyTask_arguments, METH_NOARGS,
     "Return the arguments for the task."},
    {"returns", (PyCFunction) PyTask_returns, METH_NOARGS,
     "Return the object IDs for the return values of the task."},
    {NULL} /* Sentinel */
};

PyTypeObject PyTaskType = {
    PyObject_HEAD_INIT(NULL) 0,  /* ob_size */
    "task.Task",                 /* tp_name */
    sizeof(PyTask),              /* tp_basicsize */
    0,                           /* tp_itemsize */
    (destructor) PyTask_dealloc, /* tp_dealloc */
    0,                           /* tp_print */
    0,                           /* tp_getattr */
    0,                           /* tp_setattr */
    0,                           /* tp_compare */
    0,                           /* tp_repr */
    0,                           /* tp_as_number */
    0,                           /* tp_as_sequence */
    0,                           /* tp_as_mapping */
    0,                           /* tp_hash */
    0,                           /* tp_call */
    0,                           /* tp_str */
    0,                           /* tp_getattro */
    0,                           /* tp_setattro */
    0,                           /* tp_as_buffer */
    Py_TPFLAGS_DEFAULT,          /* tp_flags */
    "Task object",               /* tp_doc */
    0,                           /* tp_traverse */
    0,                           /* tp_clear */
    0,                           /* tp_richcompare */
    0,                           /* tp_weaklistoffset */
    0,                           /* tp_iter */
    0,                           /* tp_iternext */
    PyTask_methods,              /* tp_methods */
    0,                           /* tp_members */
    0,                           /* tp_getset */
    0,                           /* tp_base */
    0,                           /* tp_dict */
    0,                           /* tp_descr_get */
    0,                           /* tp_descr_set */
    0,                           /* tp_dictoffset */
    (initproc) PyTask_init,      /* tp_init */
    0,                           /* tp_alloc */
    PyType_GenericNew,           /* tp_new */
};

/* Create a PyTask from a C struct. The resulting PyTask takes ownership of the
 * task_spec and will deallocate the task_spec in the PyTask destructor. */
PyObject *PyTask_make(task_spec *task_spec) {
  PyTask *result = PyObject_New(PyTask, &PyTaskType);
  result = (PyTask *) PyObject_Init((PyObject *) result, &PyTaskType);
  result->spec = task_spec;
  return (PyObject *) result;
}

/* Define the methods for the module. */

#define SIZE_LIMIT 100
#define NUM_ELEMENTS_LIMIT 1000

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
  if (PyString_CheckExact(value)) {
    *num_elements_contained += PyString_Size(value);
    return (*num_elements_contained < NUM_ELEMENTS_LIMIT);
  }
  if (PyUnicode_CheckExact(value)) {
    *num_elements_contained += PyUnicode_GET_SIZE(value);
    return (*num_elements_contained < NUM_ELEMENTS_LIMIT);
  }
  if (PyList_CheckExact(value) && PyList_Size(value) < SIZE_LIMIT) {
    for (size_t i = 0; i < PyList_Size(value); ++i) {
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
    for (size_t i = 0; i < PyTuple_Size(value); ++i) {
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
