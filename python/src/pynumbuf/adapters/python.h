#ifndef PYNUMBUF_PYTHON_H
#define PYNUMBUF_PYTHON_H

#include <Python.h>

#include <arrow/api.h>
#include <numbuf/dict.h>
#include <numbuf/sequence.h>

#include "numpy.h"

namespace numbuf {

arrow::Status SerializeSequences(std::vector<PyObject*> sequences, std::shared_ptr<arrow::Array>* out);
arrow::Status SerializeDict(std::vector<PyObject*> dicts, std::shared_ptr<arrow::Array>* out);
arrow::Status DeserializeList(std::shared_ptr<arrow::Array> array, int32_t start_idx, int32_t stop_idx, PyObject* base, PyObject** out);
arrow::Status DeserializeTuple(std::shared_ptr<arrow::Array> array, int32_t start_idx, int32_t stop_idx, PyObject* base, PyObject** out);
arrow::Status DeserializeDict(std::shared_ptr<arrow::Array> array, int32_t start_idx, int32_t stop_idx, PyObject* base, PyObject** out);

}

#endif
