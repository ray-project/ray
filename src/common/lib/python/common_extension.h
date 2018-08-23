#ifndef COMMON_EXTENSION_H
#define COMMON_EXTENSION_H

#include <vector>

#include <Python.h>
#include "marshal.h"
#include "structmember.h"

#include "common.h"
#include "ray/raylet/task_spec.h"

typedef char TaskSpec;
class TaskBuilder;

extern PyObject *CommonError;

// clang-format off
typedef struct {
  PyObject_HEAD
  ray::ObjectID object_id;
} PyObjectID;

typedef struct {
  PyObject_HEAD
  int64_t size;
  // The task spec to use in the non-raylet case.
  TaskSpec *spec;
  // The task spec to use in the raylet case.
  ray::raylet::TaskSpecification *task_spec;
  std::vector<ray::ObjectID> *execution_dependencies;
} PyTask;
// clang-format on

extern PyTypeObject PyObjectIDType;

extern PyTypeObject PyTaskType;

bool use_raylet(PyTask *task);

/* Python module for pickling. */
extern PyObject *pickle_module;
extern PyObject *pickle_dumps;
extern PyObject *pickle_loads;

int init_numpy_module(void);

void init_pickle_module(void);

extern TaskBuilder *g_task_builder;

int PyStringToUniqueID(PyObject *object, ray::ObjectID *object_id);

int PyObjectToUniqueID(PyObject *object, ray::ObjectID *object_id);

PyObject *PyObjectID_make(ray::ObjectID object_id);

PyObject *check_simple_value(PyObject *self, PyObject *args);
PyObject *compute_task_id(PyObject *self, PyObject *args);

PyObject *PyTask_to_string(PyObject *, PyObject *args);
PyObject *PyTask_from_string(PyObject *, PyObject *args);

PyObject *PyTask_make(TaskSpec *task_spec, int64_t task_size);

#endif /* COMMON_EXTENSION_H */
