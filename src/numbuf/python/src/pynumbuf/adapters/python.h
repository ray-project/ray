#ifndef PYNUMBUF_PYTHON_H
#define PYNUMBUF_PYTHON_H

#include <Python.h>

#include <arrow/api.h>
#include <numbuf/dict.h>
#include <numbuf/sequence.h>

#include "numpy.h"

namespace numbuf {

arrow::Status SerializeSequences(std::vector<PyObject*> sequences,
    int32_t recursion_depth, std::shared_ptr<arrow::Array>* out,
    std::vector<PyObject*>& tensors_out);
arrow::Status SerializeDict(std::vector<PyObject*> dicts, int32_t recursion_depth,
    std::shared_ptr<arrow::Array>* out,
    std::vector<PyObject*>& tensors_out);
arrow::Status DeserializeList(std::shared_ptr<arrow::Array> array, int32_t start_idx,
    int32_t stop_idx, PyObject* base, const std::vector<std::shared_ptr<arrow::Tensor>>& tensors, PyObject** out);
arrow::Status DeserializeTuple(std::shared_ptr<arrow::Array> array, int32_t start_idx,
    int32_t stop_idx, PyObject* base, const std::vector<std::shared_ptr<arrow::Tensor>>& tensors, PyObject** out);
arrow::Status DeserializeDict(std::shared_ptr<arrow::Array> array, int32_t start_idx,
    int32_t stop_idx, PyObject* base, const std::vector<std::shared_ptr<arrow::Tensor>>& tensors, PyObject** out);
}

#endif
