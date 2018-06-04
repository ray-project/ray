#ifndef COMMON_EXTENSION_H
#define COMMON_EXTENSION_H

#include <Python.h>
#include <vector>
#include "marshal.h"
#include "structmember.h"

#include "common.h"
#include "ray/raylet/task_spec.h"

typedef char TaskSpec;
class TaskBuilder;

extern PyObject *CommonError;

extern PyTypeObject PyObjectIDType;

extern PyTypeObject PyTaskType;
/* Python module for pickling. */
extern PyObject *pickle_module;
extern PyObject *pickle_dumps;
extern PyObject *pickle_loads;

int init_numpy_module(void);

void init_pickle_module(void);

int PyStringToUniqueID(PyObject *object, ray::ObjectID *object_id);

int PyObjectToUniqueID(PyObject *object, ray::ObjectID *object_id);

PyObject *PyObjectID_make(ray::ObjectID object_id);

PyObject *check_simple_value(PyObject *self, PyObject *args);

PyObject *PyTask_to_string(PyObject *, PyObject *args);
PyObject *PyTask_from_string(PyObject *, PyObject *args);

PyObject *PyTask_make(TaskSpec *task_spec, int64_t task_size);

#endif /* COMMON_EXTENSION_H */
