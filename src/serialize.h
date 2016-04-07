#ifndef ORCHESTRA_SERIALIZE_H
#define ORCHESTRA_SERIALIZE_H

#define NPY_NO_DEPRECATED_API NPY_1_7_API_VERSION

#include <arrow/api.h>
#include <arrow/ipc/memory.h>
#include <arrow/ipc/adapter.h>
#include <Python.h>
#define NO_IMPORT_ARRAY
#define PY_ARRAY_UNIQUE_SYMBOL ORCHESTRA_ARRAY_API
#include <numpy/arrayobject.h>
#include <memory>

#include "ipc.h"

size_t arrow_size(PyArrayObject* array);
void store_arrow(PyArrayObject* array, ObjHandle& location, MemorySegmentPool* pool);
PyObject* deserialize_array(ObjHandle handle, MemorySegmentPool* pool);

#endif
