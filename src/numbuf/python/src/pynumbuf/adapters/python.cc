#include "python.h"

#include <sstream>

#include "scalars.h"

using namespace arrow;

int32_t MAX_RECURSION_DEPTH = 100;

extern "C" {

extern PyObject* numbuf_serialize_callback;
extern PyObject* numbuf_deserialize_callback;
}

namespace numbuf {

#if PY_MAJOR_VERSION >= 3
#define PyInt_FromLong PyLong_FromLong
#endif

Status get_value(std::shared_ptr<Array> arr, int32_t index, int32_t type, PyObject* base,
    const std::vector<std::shared_ptr<Tensor>>& tensors, PyObject** result) {
  switch (arr->type()->id()) {
    case Type::BOOL:
      *result =
          PyBool_FromLong(std::static_pointer_cast<BooleanArray>(arr)->Value(index));
      return Status::OK();
    case Type::INT64:
      *result = PyInt_FromLong(std::static_pointer_cast<Int64Array>(arr)->Value(index));
      return Status::OK();
    case Type::BINARY: {
      int32_t nchars;
      const uint8_t* str =
          std::static_pointer_cast<BinaryArray>(arr)->GetValue(index, &nchars);
      *result = PyBytes_FromStringAndSize(reinterpret_cast<const char*>(str), nchars);
      return Status::OK();
    }
    case Type::STRING: {
      int32_t nchars;
      const uint8_t* str =
          std::static_pointer_cast<StringArray>(arr)->GetValue(index, &nchars);
      *result = PyUnicode_FromStringAndSize(reinterpret_cast<const char*>(str), nchars);
      return Status::OK();
    }
    case Type::FLOAT:
      *result =
          PyFloat_FromDouble(std::static_pointer_cast<FloatArray>(arr)->Value(index));
      return Status::OK();
    case Type::DOUBLE:
      *result =
          PyFloat_FromDouble(std::static_pointer_cast<DoubleArray>(arr)->Value(index));
      return Status::OK();
    case Type::STRUCT: {
      auto s = std::static_pointer_cast<StructArray>(arr);
      auto l = std::static_pointer_cast<ListArray>(s->field(0));
      if (s->type()->child(0)->name() == "list") {
        return DeserializeList(l->values(), l->value_offset(index),
            l->value_offset(index + 1), base, tensors, result);
      } else if (s->type()->child(0)->name() == "tuple") {
        return DeserializeTuple(l->values(), l->value_offset(index),
            l->value_offset(index + 1), base, tensors, result);
      } else if (s->type()->child(0)->name() == "dict") {
        return DeserializeDict(l->values(), l->value_offset(index),
            l->value_offset(index + 1), base, tensors, result);
      } else {
        DCHECK(false) << "error";
      }
    }
    // We use an Int32Builder here to distinguish the tensor indices from
    // the Type::INT64 above (see tensor_indices_ in sequence.h).
    case Type::INT32: {
      int32_t val = std::static_pointer_cast<Int32Array>(arr)->Value(index);
      return DeserializeArray(arr, index, base, tensors, result);
    }
    default:
      DCHECK(false) << "union tag not recognized " << type;
  }
  return Status::OK();
}

Status append(PyObject* elem, SequenceBuilder& builder, std::vector<PyObject*>& sublists,
    std::vector<PyObject*>& subtuples, std::vector<PyObject*>& subdicts,
    std::vector<PyObject*>& tensors_out) {
  // The bool case must precede the int case (PyInt_Check passes for bools)
  if (PyBool_Check(elem)) {
    RETURN_NOT_OK(builder.AppendBool(elem == Py_True));
  } else if (PyFloat_Check(elem)) {
    RETURN_NOT_OK(builder.AppendDouble(PyFloat_AS_DOUBLE(elem)));
  } else if (PyLong_Check(elem)) {
    int overflow = 0;
    int64_t data = PyLong_AsLongLongAndOverflow(elem, &overflow);
    RETURN_NOT_OK(builder.AppendInt64(data));
    if (overflow) { return Status::NotImplemented("long overflow"); }
#if PY_MAJOR_VERSION < 3
  } else if (PyInt_Check(elem)) {
    RETURN_NOT_OK(builder.AppendInt64(static_cast<int64_t>(PyInt_AS_LONG(elem))));
#endif
  } else if (PyBytes_Check(elem)) {
    auto data = reinterpret_cast<uint8_t*>(PyBytes_AS_STRING(elem));
    auto size = PyBytes_GET_SIZE(elem);
    RETURN_NOT_OK(builder.AppendBytes(data, size));
  } else if (PyUnicode_Check(elem)) {
    Py_ssize_t size;
#if PY_MAJOR_VERSION >= 3
    char* data = PyUnicode_AsUTF8AndSize(elem, &size);
    Status s = builder.AppendString(data, size);
#else
    PyObject* str = PyUnicode_AsUTF8String(elem);
    char* data = PyString_AS_STRING(str);
    size = PyString_GET_SIZE(str);
    Status s = builder.AppendString(data, size);
    Py_XDECREF(str);
#endif
    RETURN_NOT_OK(s);
  } else if (PyList_Check(elem)) {
    builder.AppendList(PyList_Size(elem));
    sublists.push_back(elem);
  } else if (PyDict_Check(elem)) {
    builder.AppendDict(PyDict_Size(elem));
    subdicts.push_back(elem);
  } else if (PyTuple_CheckExact(elem)) {
    builder.AppendTuple(PyTuple_Size(elem));
    subtuples.push_back(elem);
  } else if (PyArray_IsScalar(elem, Generic)) {
    RETURN_NOT_OK(AppendScalar(elem, builder));
  } else if (PyArray_Check(elem)) {
    RETURN_NOT_OK(SerializeArray((PyArrayObject*)elem, builder, subdicts, tensors_out));
  } else if (elem == Py_None) {
    RETURN_NOT_OK(builder.AppendNone());
  } else {
    if (!numbuf_serialize_callback) {
      std::stringstream ss;
      ss << "data type of " << PyBytes_AS_STRING(PyObject_Repr(elem))
         << " not recognized and custom serialization handler not registered";
      return Status::NotImplemented(ss.str());
    } else {
      PyObject* arglist = Py_BuildValue("(O)", elem);
      // The reference count of the result of the call to PyObject_CallObject
      // must be decremented. This is done in SerializeDict in this file.
      PyObject* result = PyObject_CallObject(numbuf_serialize_callback, arglist);
      Py_XDECREF(arglist);
      if (!result) { return Status::NotImplemented("python error"); }
      builder.AppendDict(PyDict_Size(result));
      subdicts.push_back(result);
    }
  }
  return Status::OK();
}

Status SerializeSequences(std::vector<PyObject*> sequences, int32_t recursion_depth,
    std::shared_ptr<Array>* out, std::vector<PyObject*>& tensors_out) {
  DCHECK(out);
  if (recursion_depth >= MAX_RECURSION_DEPTH) {
    return Status::NotImplemented(
        "This object exceeds the maximum recursion depth. It may contain itself "
        "recursively.");
  }
  SequenceBuilder builder(nullptr);
  std::vector<PyObject *> sublists, subtuples, subdicts;
  for (const auto& sequence : sequences) {
    PyObject* item;
    PyObject* iterator = PyObject_GetIter(sequence);
    while ((item = PyIter_Next(iterator))) {
      Status s = append(item, builder, sublists, subtuples, subdicts, tensors_out);
      Py_DECREF(item);
      // if an error occurs, we need to decrement the reference counts before returning
      if (!s.ok()) {
        Py_DECREF(iterator);
        return s;
      }
    }
    Py_DECREF(iterator);
  }
  std::shared_ptr<Array> list;
  if (sublists.size() > 0) {
    RETURN_NOT_OK(SerializeSequences(sublists, recursion_depth + 1, &list, tensors_out));
  }
  std::shared_ptr<Array> tuple;
  if (subtuples.size() > 0) {
    RETURN_NOT_OK(
        SerializeSequences(subtuples, recursion_depth + 1, &tuple, tensors_out));
  }
  std::shared_ptr<Array> dict;
  if (subdicts.size() > 0) {
    RETURN_NOT_OK(SerializeDict(subdicts, recursion_depth + 1, &dict, tensors_out));
  }
  return builder.Finish(list, tuple, dict, out);
}

#define DESERIALIZE_SEQUENCE(CREATE, SET_ITEM)                              \
  auto data = std::dynamic_pointer_cast<UnionArray>(array);                 \
  int32_t size = array->length();                                           \
  PyObject* result = CREATE(stop_idx - start_idx);                          \
  auto types = std::make_shared<Int8Array>(size, data->type_ids());         \
  auto offsets = std::make_shared<Int32Array>(size, data->value_offsets()); \
  for (size_t i = start_idx; i < stop_idx; ++i) {                           \
    if (data->IsNull(i)) {                                                  \
      Py_INCREF(Py_None);                                                   \
      SET_ITEM(result, i - start_idx, Py_None);                             \
    } else {                                                                \
      int32_t offset = offsets->Value(i);                                   \
      int8_t type = types->Value(i);                                        \
      std::shared_ptr<Array> arr = data->child(type);                       \
      PyObject* value;                                                      \
      RETURN_NOT_OK(get_value(arr, offset, type, base, tensors, &value));   \
      SET_ITEM(result, i - start_idx, value);                               \
    }                                                                       \
  }                                                                         \
  *out = result;                                                            \
  return Status::OK();

Status DeserializeList(std::shared_ptr<Array> array, int32_t start_idx, int32_t stop_idx,
    PyObject* base, const std::vector<std::shared_ptr<Tensor>>& tensors, PyObject** out) {
  DESERIALIZE_SEQUENCE(PyList_New, PyList_SetItem)
}

Status DeserializeTuple(std::shared_ptr<Array> array, int32_t start_idx, int32_t stop_idx,
    PyObject* base, const std::vector<std::shared_ptr<Tensor>>& tensors, PyObject** out) {
  DESERIALIZE_SEQUENCE(PyTuple_New, PyTuple_SetItem)
}

Status SerializeDict(std::vector<PyObject*> dicts, int32_t recursion_depth,
    std::shared_ptr<Array>* out, std::vector<PyObject*>& tensors_out) {
  DictBuilder result;
  if (recursion_depth >= MAX_RECURSION_DEPTH) {
    return Status::NotImplemented(
        "This object exceeds the maximum recursion depth. It may contain itself "
        "recursively.");
  }
  std::vector<PyObject *> key_tuples, key_dicts, val_lists, val_tuples, val_dicts, dummy;
  for (const auto& dict : dicts) {
    PyObject *key, *value;
    Py_ssize_t pos = 0;
    while (PyDict_Next(dict, &pos, &key, &value)) {
      RETURN_NOT_OK(append(key, result.keys(), dummy, key_tuples, key_dicts, tensors_out));
      DCHECK(dummy.size() == 0);
      RETURN_NOT_OK(
          append(value, result.vals(), val_lists, val_tuples, val_dicts, tensors_out));
    }
  }
  std::shared_ptr<Array> key_tuples_arr;
  if (key_tuples.size() > 0) {
    RETURN_NOT_OK(SerializeSequences(
        key_tuples, recursion_depth + 1, &key_tuples_arr, tensors_out));
  }
  std::shared_ptr<Array> key_dicts_arr;
  if (key_dicts.size() > 0) {
    RETURN_NOT_OK(
        SerializeDict(key_dicts, recursion_depth + 1, &key_dicts_arr, tensors_out));
  }
  std::shared_ptr<Array> val_list_arr;
  if (val_lists.size() > 0) {
    RETURN_NOT_OK(
        SerializeSequences(val_lists, recursion_depth + 1, &val_list_arr, tensors_out));
  }
  std::shared_ptr<Array> val_tuples_arr;
  if (val_tuples.size() > 0) {
    RETURN_NOT_OK(SerializeSequences(
        val_tuples, recursion_depth + 1, &val_tuples_arr, tensors_out));
  }
  std::shared_ptr<Array> val_dict_arr;
  if (val_dicts.size() > 0) {
    RETURN_NOT_OK(
        SerializeDict(val_dicts, recursion_depth + 1, &val_dict_arr, tensors_out));
  }
  result.Finish(key_tuples_arr, key_dicts_arr, val_list_arr, val_tuples_arr, val_dict_arr, out);

  // This block is used to decrement the reference counts of the results
  // returned by the serialization callback, which is called in SerializeArray
  // in numpy.cc as well as in DeserializeDict and in append in this file.
  static PyObject* py_type = PyUnicode_FromString("_pytype_");
  for (const auto& dict : dicts) {
    if (PyDict_Contains(dict, py_type)) {
      // If the dictionary contains the key "_pytype_", then the user has to
      // have registered a callback.
      ARROW_CHECK(numbuf_serialize_callback);
      Py_XDECREF(dict);
    }
  }

  return Status::OK();
}

Status DeserializeDict(std::shared_ptr<Array> array, int32_t start_idx, int32_t stop_idx,
    PyObject* base, const std::vector<std::shared_ptr<Tensor>>& tensors, PyObject** out) {
  auto data = std::dynamic_pointer_cast<StructArray>(array);
  // TODO(pcm): error handling, get rid of the temporary copy of the list
  PyObject *keys, *vals;
  PyObject* result = PyDict_New();
  ARROW_RETURN_NOT_OK(
      DeserializeList(data->field(0), start_idx, stop_idx, base, tensors, &keys));
  ARROW_RETURN_NOT_OK(
      DeserializeList(data->field(1), start_idx, stop_idx, base, tensors, &vals));
  for (size_t i = start_idx; i < stop_idx; ++i) {
    PyDict_SetItem(
        result, PyList_GetItem(keys, i - start_idx), PyList_GetItem(vals, i - start_idx));
  }
  Py_XDECREF(keys);  // PyList_GetItem(keys, ...) incremented the reference count
  Py_XDECREF(vals);  // PyList_GetItem(vals, ...) incremented the reference count
  static PyObject* py_type = PyUnicode_FromString("_pytype_");
  if (PyDict_Contains(result, py_type) && numbuf_deserialize_callback) {
    PyObject* arglist = Py_BuildValue("(O)", result);
    // The result of the call to PyObject_CallObject will be passed to Python
    // and its reference count will be decremented by the interpreter.
    PyObject* callback_result = PyObject_CallObject(numbuf_deserialize_callback, arglist);
    Py_XDECREF(arglist);
    Py_XDECREF(result);
    result = callback_result;
    if (!callback_result) { return Status::NotImplemented("python error"); }
  }
  *out = result;
  return Status::OK();
}
}
