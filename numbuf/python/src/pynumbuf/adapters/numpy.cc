#include "numpy.h"
#include "python.h"

#include <sstream>

#include <numbuf/tensor.h>

using namespace arrow;

extern "C" {
extern PyObject* numbuf_serialize_callback;
extern PyObject* numbuf_deserialize_callback;
}

namespace numbuf {

#define ARROW_TYPE_TO_NUMPY_CASE(TYPE) \
  case Type::TYPE:                     \
    return NPY_##TYPE;

#define DESERIALIZE_ARRAY_CASE(TYPE, ArrayType, type)                             \
  case Type::TYPE: {                                                              \
    auto values = std::dynamic_pointer_cast<ArrayType>(content->values());        \
    DCHECK(values);                                                               \
    type* data = const_cast<type*>(values->raw_data()) + content->offset(offset); \
    *out = PyArray_SimpleNewFromData(                                             \
        num_dims, dim.data(), NPY_##TYPE, reinterpret_cast<void*>(data));         \
    if (base != Py_None) { PyArray_SetBaseObject((PyArrayObject*)*out, base); }   \
    Py_XINCREF(base);                                                             \
  }                                                                               \
    return Status::OK();

Status DeserializeArray(
    std::shared_ptr<Array> array, int32_t offset, PyObject* base, PyObject** out) {
  DCHECK(array);
  auto tensor = std::dynamic_pointer_cast<StructArray>(array);
  DCHECK(tensor);
  auto dims = std::dynamic_pointer_cast<ListArray>(tensor->field(0));
  auto content = std::dynamic_pointer_cast<ListArray>(tensor->field(1));
  npy_intp num_dims = dims->value_length(offset);
  std::vector<npy_intp> dim(num_dims);
  for (int i = dims->offset(offset); i < dims->offset(offset + 1); ++i) {
    dim[i - dims->offset(offset)] =
        std::dynamic_pointer_cast<Int64Array>(dims->values())->Value(i);
  }
  switch (content->value_type()->type) {
    DESERIALIZE_ARRAY_CASE(INT8, Int8Array, int8_t)
    DESERIALIZE_ARRAY_CASE(INT16, Int16Array, int16_t)
    DESERIALIZE_ARRAY_CASE(INT32, Int32Array, int32_t)
    DESERIALIZE_ARRAY_CASE(INT64, Int64Array, int64_t)
    DESERIALIZE_ARRAY_CASE(UINT8, UInt8Array, uint8_t)
    DESERIALIZE_ARRAY_CASE(UINT16, UInt16Array, uint16_t)
    DESERIALIZE_ARRAY_CASE(UINT32, UInt32Array, uint32_t)
    DESERIALIZE_ARRAY_CASE(UINT64, UInt64Array, uint64_t)
    DESERIALIZE_ARRAY_CASE(FLOAT, FloatArray, float)
    DESERIALIZE_ARRAY_CASE(DOUBLE, DoubleArray, double)
    default:
      DCHECK(false) << "arrow type not recognized: " << content->value_type()->type;
  }
  return Status::OK();
}

Status SerializeArray(
    PyArrayObject* array, SequenceBuilder& builder, std::vector<PyObject*>& subdicts) {
  size_t ndim = PyArray_NDIM(array);
  int dtype = PyArray_TYPE(array);
  std::vector<int64_t> dims(ndim);
  for (int i = 0; i < ndim; ++i) {
    dims[i] = PyArray_DIM(array, i);
  }
  // TODO(pcm): Once we don't use builders any more below and directly share
  // the memory buffer, we need to be more careful about this and not
  // decrease the reference count of "contiguous" before the serialization
  // is finished
  auto contiguous = PyArray_GETCONTIGUOUS(array);
  auto data = PyArray_DATA(contiguous);
  switch (dtype) {
    case NPY_UINT8:
      RETURN_NOT_OK(builder.AppendTensor(dims, reinterpret_cast<uint8_t*>(data)));
      break;
    case NPY_INT8:
      RETURN_NOT_OK(builder.AppendTensor(dims, reinterpret_cast<int8_t*>(data)));
      break;
    case NPY_UINT16:
      RETURN_NOT_OK(builder.AppendTensor(dims, reinterpret_cast<uint16_t*>(data)));
      break;
    case NPY_INT16:
      RETURN_NOT_OK(builder.AppendTensor(dims, reinterpret_cast<int16_t*>(data)));
      break;
    case NPY_UINT32:
      RETURN_NOT_OK(builder.AppendTensor(dims, reinterpret_cast<uint32_t*>(data)));
      break;
    case NPY_INT32:
      RETURN_NOT_OK(builder.AppendTensor(dims, reinterpret_cast<int32_t*>(data)));
      break;
    case NPY_UINT64:
      RETURN_NOT_OK(builder.AppendTensor(dims, reinterpret_cast<uint64_t*>(data)));
      break;
    case NPY_INT64:
      RETURN_NOT_OK(builder.AppendTensor(dims, reinterpret_cast<int64_t*>(data)));
      break;
    case NPY_FLOAT:
      RETURN_NOT_OK(builder.AppendTensor(dims, reinterpret_cast<float*>(data)));
      break;
    case NPY_DOUBLE:
      RETURN_NOT_OK(builder.AppendTensor(dims, reinterpret_cast<double*>(data)));
      break;
    default:
      if (!numbuf_serialize_callback) {
        std::stringstream stream;
        stream << "numpy data type not recognized: " << dtype;
        return Status::NotImplemented(stream.str());
      } else {
        PyObject* arglist = Py_BuildValue("(O)", array);
        // The reference count of the result of the call to PyObject_CallObject
        // must be decremented. This is done in SerializeDict in python.cc.
        PyObject* result = PyObject_CallObject(numbuf_serialize_callback, arglist);
        Py_XDECREF(arglist);
        if (!result) { return Status::NotImplemented("python error"); }
        builder.AppendDict(PyDict_Size(result));
        subdicts.push_back(result);
      }
  }
  Py_XDECREF(contiguous);
  return Status::OK();
}
}
