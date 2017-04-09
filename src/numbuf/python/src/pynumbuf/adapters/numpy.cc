#include "numpy.h"
#include "python.h"

#include <sstream>

#include <arrow/python/numpy_convert.h>

using namespace arrow;

extern "C" {
extern PyObject* numbuf_serialize_callback;
extern PyObject* numbuf_deserialize_callback;
}

namespace numbuf {

Status DeserializeArray(
    std::shared_ptr<Array> array, int32_t offset, PyObject* base, const std::vector<std::shared_ptr<arrow::Tensor>>& tensors, PyObject** out) {
  DCHECK(array);
  int32_t index = std::static_pointer_cast<Int32Array>(array)->Value(offset);
  RETURN_NOT_OK(py::TensorToNdarray(*tensors[index], base, out));
  /* Mark the array as immutable. */
  PyObject* flags = PyObject_GetAttrString(*out, "flags");
  DCHECK(flags != NULL) << "Could not mark Numpy array immutable";
  int flag_set = PyObject_SetAttrString(flags, "writeable", Py_False);
  DCHECK(flag_set == 0) << "Could not mark Numpy array immutable";
  Py_XDECREF(flags);
  return Status::OK();
}

Status SerializeArray(
    PyArrayObject* array, SequenceBuilder& builder, std::vector<PyObject*>& subdicts,
    std::vector<std::shared_ptr<Tensor>>& tensors_out) {
  int dtype = PyArray_TYPE(array);
  switch (dtype) {
    case NPY_UINT8:
    case NPY_INT8:
    case NPY_UINT16:
    case NPY_INT16:
    case NPY_UINT32:
    case NPY_INT32:
    case NPY_UINT64:
    case NPY_INT64:
    case NPY_FLOAT:
    case NPY_DOUBLE: {
      RETURN_NOT_OK(builder.AppendTensor(tensors_out.size()));
      std::shared_ptr<Tensor> tensor;
      RETURN_NOT_OK(py::NdarrayToTensor(NULL, reinterpret_cast<PyObject*>(array), &tensor));
      tensors_out.push_back(tensor);
    }
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
  return Status::OK();
}
}
