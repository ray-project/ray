#include "python.h"

#include <sstream>

#include "scalars.h"

using namespace arrow;

namespace numbuf {

PyObject* get_value(ArrayPtr arr, int32_t index, int32_t type) {
  PyObject* result;
  switch (arr->type()->type) {
    case Type::BOOL:
      return PyBool_FromLong(std::static_pointer_cast<BooleanArray>(arr)->Value(index));
    case Type::INT64:
      return PyInt_FromLong(std::static_pointer_cast<Int64Array>(arr)->Value(index));
    case Type::STRING: {
      int32_t nchars;
      const uint8_t* str = std::static_pointer_cast<StringArray>(arr)->GetValue(index, &nchars);
      return PyString_FromStringAndSize(reinterpret_cast<const char*>(str), nchars);
    }
    case Type::FLOAT:
      return PyFloat_FromDouble(std::static_pointer_cast<FloatArray>(arr)->Value(index));
    case Type::DOUBLE:
      return PyFloat_FromDouble(std::static_pointer_cast<DoubleArray>(arr)->Value(index));
    case Type::STRUCT: {
      auto s = std::static_pointer_cast<StructArray>(arr);
      auto l = std::static_pointer_cast<ListArray>(s->field(0));
      if (s->type()->child(0)->name == "list") {
        ARROW_CHECK_OK(DeserializeList(l->values(), l->value_offset(index), l->value_offset(index+1), &result));
      } else if (s->type()->child(0)->name == "tuple") {
        ARROW_CHECK_OK(DeserializeTuple(l->values(), l->value_offset(index), l->value_offset(index+1), &result));
      } else if (s->type()->child(0)->name == "dict") {
        ARROW_CHECK_OK(DeserializeDict(l->values(), l->value_offset(index), l->value_offset(index+1), &result));
      } else {
        ARROW_CHECK_OK(DeserializeArray(arr, index, &result));
      }
      return result;
    }
    default:
      DCHECK(false) << "union tag not recognized " << type;
  }
  return NULL;
}

Status append(PyObject* elem, SequenceBuilder& builder,
              std::vector<PyObject*>& sublists,
              std::vector<PyObject*>& subtuples,
              std::vector<PyObject*>& subdicts) {
  // The bool case must precede the int case (PyInt_Check passes for bools)
  if (PyBool_Check(elem)) {
    RETURN_NOT_OK(builder.Append(elem == Py_True));
  } else if (PyFloat_Check(elem)) {
    RETURN_NOT_OK(builder.Append(PyFloat_AS_DOUBLE(elem)));
  } else if (PyLong_Check(elem)) {
    int overflow = 0;
    int64_t data = PyLong_AsLongLongAndOverflow(elem, &overflow);
    RETURN_NOT_OK(builder.Append(data));
    if(overflow) {
      return Status::NotImplemented("long overflow");
    }
  } else if (PyInt_Check(elem)) {
    RETURN_NOT_OK(builder.Append(PyInt_AS_LONG(elem)));
  } else if (PyString_Check(elem)) {
    RETURN_NOT_OK(builder.Append(PyString_AS_STRING(elem), PyString_GET_SIZE(elem)));
  } else if (PyList_Check(elem)) {
    builder.AppendList(PyList_Size(elem));
    sublists.push_back(elem);
  } else if (PyDict_Check(elem)) {
    builder.AppendDict(PyDict_Size(elem));
    subdicts.push_back(elem);
  } else if (PyTuple_Check(elem)) {
    builder.AppendTuple(PyTuple_Size(elem));
    subtuples.push_back(elem);
  } else if (PyArray_IsScalar(elem, Generic)) {
    RETURN_NOT_OK(AppendScalar(elem, builder));
  } else if (PyArray_Check(elem)) {
    RETURN_NOT_OK(SerializeArray((PyArrayObject*) elem, builder));
  } else if (elem == Py_None) {
    RETURN_NOT_OK(builder.Append());
  } else {
    std::stringstream ss;
    ss << "data type of " << PyString_AS_STRING(PyObject_Repr(elem))
       << " not recognized";
    return Status::NotImplemented(ss.str());
  }
  return Status::OK();
}

Status SerializeSequences(std::vector<PyObject*> sequences, std::shared_ptr<Array>* out) {
  DCHECK(out);
  SequenceBuilder builder(nullptr);
  std::vector<PyObject*> sublists, subtuples, subdicts;
  for (const auto& sequence : sequences) {
    PyObject* item;
    PyObject* iterator = PyObject_GetIter(sequence);
    while (item = PyIter_Next(iterator)) {
      RETURN_NOT_OK(append(item, builder, sublists, subtuples, subdicts));
      Py_DECREF(item);
    }
    Py_DECREF(iterator);
  }
  std::shared_ptr<Array> list;
  if (sublists.size() > 0) {
    RETURN_NOT_OK(SerializeSequences(sublists, &list));
  }
  std::shared_ptr<Array> tuple;
  if (subtuples.size() > 0) {
    RETURN_NOT_OK(SerializeSequences(subtuples, &tuple));
  }
  std::shared_ptr<Array> dict;
  if (subdicts.size() > 0) {
    RETURN_NOT_OK(SerializeDict(subdicts, &dict));
  }
  *out = builder.Finish(list, tuple, dict);
  return Status::OK();
}

#define DESERIALIZE_SEQUENCE(CREATE, SET_ITEM)                                \
  auto data = std::dynamic_pointer_cast<DenseUnionArray>(array);              \
  int32_t size = array->length();                                             \
  PyObject* result = CREATE(stop_idx - start_idx);                            \
  auto types = std::make_shared<Int8Array>(size, data->types());              \
  auto offsets = std::make_shared<Int32Array>(size, data->offset_buf());      \
  for (size_t i = start_idx; i < stop_idx; ++i) {                             \
    if (data->IsNull(i)) {                                                    \
      Py_INCREF(Py_None);                                                     \
      SET_ITEM(result, i-start_idx, Py_None);                                 \
    } else {                                                                  \
      int32_t offset = offsets->Value(i);                                     \
      int8_t type = types->Value(i);                                          \
      ArrayPtr arr = data->child(type);                                       \
      SET_ITEM(result, i-start_idx, get_value(arr, offset, type));            \
    }                                                                         \
  }                                                                           \
  *out = result;                                                              \
  return Status::OK();

Status DeserializeList(std::shared_ptr<Array> array, int32_t start_idx, int32_t stop_idx, PyObject** out) {
  DESERIALIZE_SEQUENCE(PyList_New, PyList_SetItem)
}

Status DeserializeTuple(std::shared_ptr<Array> array, int32_t start_idx, int32_t stop_idx, PyObject** out) {
  DESERIALIZE_SEQUENCE(PyTuple_New, PyTuple_SetItem)
}

Status SerializeDict(std::vector<PyObject*> dicts, std::shared_ptr<Array>* out) {
  DictBuilder result;
  std::vector<PyObject*> sublists, subtuples, subdicts, dummy;
  for (const auto& dict : dicts) {
    PyObject *key, *value;
    Py_ssize_t pos = 0;
    while (PyDict_Next(dict, &pos, &key, &value)) {
      RETURN_NOT_OK(append(key, result.keys(), dummy, dummy, dummy));
      DCHECK(dummy.size() == 0);
      RETURN_NOT_OK(append(value, result.vals(), sublists, subtuples, subdicts));
    }
  }
  std::shared_ptr<Array> val_list;
  if (sublists.size() > 0) {
    RETURN_NOT_OK(SerializeSequences(sublists, &val_list));
  }
  std::shared_ptr<Array> val_tuples;
  if (subtuples.size() > 0) {
    RETURN_NOT_OK(SerializeSequences(subtuples, &val_tuples));
  }
  std::shared_ptr<Array> val_dict;
  if (subdicts.size() > 0) {
    RETURN_NOT_OK(SerializeDict(subdicts, &val_dict));
  }
  *out = result.Finish(val_list, val_tuples, val_dict);
  return Status::OK();
}

Status DeserializeDict(std::shared_ptr<Array> array, int32_t start_idx, int32_t stop_idx, PyObject** out) {
  auto data = std::dynamic_pointer_cast<StructArray>(array);
  // TODO(pcm): error handling, get rid of the temporary copy of the list
  PyObject *keys, *vals;
  PyObject* result = PyDict_New();
  ARROW_RETURN_NOT_OK(DeserializeList(data->field(0), start_idx, stop_idx, &keys));
  ARROW_RETURN_NOT_OK(DeserializeList(data->field(1), start_idx, stop_idx, &vals));
  for (size_t i = start_idx; i < stop_idx; ++i) {
    PyDict_SetItem(result, PyList_GetItem(keys, i - start_idx), PyList_GetItem(vals, i - start_idx));
  }
  Py_XDECREF(keys); // PyList_GetItem(keys, ...) incremented the reference count
  Py_XDECREF(vals); // PyList_GetItem(vals, ...) incremented the reference count
  *out = result;
  return Status::OK();
}


}
