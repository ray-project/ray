#ifndef PYNUMBUF_SCALARS_H
#define PYNUMBUF_SCALARS_H

#include <arrow/api.h>

#include <Python.h>
#define NPY_NO_DEPRECATED_API NPY_1_7_API_VERSION
#define NO_IMPORT_ARRAY
#define PY_ARRAY_UNIQUE_SYMBOL arrow_ARRAY_API
#include <numpy/arrayobject.h>
#include <numpy/arrayscalars.h>

#include <numbuf/sequence.h>

namespace numbuf {

arrow::Status AppendScalar(PyObject* obj, SequenceBuilder& builder) {
  if (PyArray_IsScalar(obj, Bool)) {
    return builder.AppendBool(((PyBoolScalarObject*)obj)->obval != 0);
  } else if (PyArray_IsScalar(obj, Float)) {
    return builder.AppendFloat(((PyFloatScalarObject*)obj)->obval);
  } else if (PyArray_IsScalar(obj, Double)) {
    return builder.AppendDouble(((PyDoubleScalarObject*)obj)->obval);
  }
  int64_t value = 0;
  if (PyArray_IsScalar(obj, Byte)) {
    value = ((PyByteScalarObject*)obj)->obval;
  } else if (PyArray_IsScalar(obj, UByte)) {
    value = ((PyUByteScalarObject*)obj)->obval;
  } else if (PyArray_IsScalar(obj, Short)) {
    value = ((PyShortScalarObject*)obj)->obval;
  } else if (PyArray_IsScalar(obj, UShort)) {
    value = ((PyUShortScalarObject*)obj)->obval;
  } else if (PyArray_IsScalar(obj, Int)) {
    value = ((PyIntScalarObject*)obj)->obval;
  } else if (PyArray_IsScalar(obj, UInt)) {
    value = ((PyUIntScalarObject*)obj)->obval;
  } else if (PyArray_IsScalar(obj, Long)) {
    value = ((PyLongScalarObject*)obj)->obval;
  } else if (PyArray_IsScalar(obj, ULong)) {
    value = ((PyULongScalarObject*)obj)->obval;
  } else if (PyArray_IsScalar(obj, LongLong)) {
    value = ((PyLongLongScalarObject*)obj)->obval;
  } else if (PyArray_IsScalar(obj, ULongLong)) {
    value = ((PyULongLongScalarObject*)obj)->obval;
  } else {
    DCHECK(false) << "scalar type not recognized";
  }
  return builder.AppendInt64(value);
}

}  // namespace

#endif  // PYNUMBUF_SCALARS_H
