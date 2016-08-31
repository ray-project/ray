#ifndef PYNUMBUF_NUMPY_H
#define PYNUMBUF_NUMPY_H

#include <arrow/api.h>
#include <Python.h>

#define NPY_NO_DEPRECATED_API NPY_1_7_API_VERSION
#define NO_IMPORT_ARRAY
#define PY_ARRAY_UNIQUE_SYMBOL NUMBUF_ARRAY_API
#include <numpy/arrayobject.h>

#include <numbuf/tensor.h>
#include <numbuf/sequence.h>

namespace numbuf {

arrow::Status SerializeArray(PyArrayObject* array, SequenceBuilder& builder, std::vector<PyObject*>& subdicts);
arrow::Status DeserializeArray(std::shared_ptr<arrow::Array> array, int32_t offset, PyObject** out);

}

#endif
