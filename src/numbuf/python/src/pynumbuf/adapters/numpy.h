#ifndef PYNUMBUF_NUMPY_H
#define PYNUMBUF_NUMPY_H

#include <Python.h>
#include <arrow/api.h>

#define NPY_NO_DEPRECATED_API NPY_1_7_API_VERSION
#define NO_IMPORT_ARRAY
#define PY_ARRAY_UNIQUE_SYMBOL arrow_ARRAY_API
#include <numpy/arrayobject.h>

#include <numbuf/sequence.h>

namespace numbuf {

arrow::Status SerializeArray(
    PyArrayObject* array, SequenceBuilder& builder, std::vector<PyObject*>& subdicts,
    std::vector<PyObject*>& tensors_out);
arrow::Status DeserializeArray(
    std::shared_ptr<arrow::Array> array, int32_t offset, PyObject* base, const std::vector<std::shared_ptr<arrow::Tensor>>& tensors, PyObject** out);
}

#endif
