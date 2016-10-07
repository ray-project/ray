#ifndef COMMON_EXTENSION_H
#define COMMON_EXTENSION_H

#include <Python.h>
#include "marshal.h"
#include "structmember.h"

#include "common.h"
#include "task.h"

extern PyObject *CommonError;

// clang-format off
typedef struct {
  PyObject_HEAD
  object_id object_id;
} PyObjectID;

typedef struct {
  PyObject_HEAD
  task_spec *spec;
} PyTask;
// clang-format on

extern PyTypeObject PyObjectIDType;

extern PyTypeObject PyTaskType;

int PyObjectToUniqueID(PyObject *object, object_id *objectid);

PyObject *PyObjectID_make(object_id object_id);

PyObject *check_simple_value(PyObject *self, PyObject *args);

PyObject *PyTask_make(task_spec *task_spec);

#endif /* COMMON_EXTENSION_H */
