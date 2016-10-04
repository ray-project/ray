#include "types.h"

/* TODO(pcm): Add limit on total number of elements. */

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
