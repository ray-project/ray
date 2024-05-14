// Copyright 2024 The Ray Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//  http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#pragma once

#include <Python.h>

#include <functional>
#include <string>
#include <type_traits>

#include "ray/common/status.h"
#include "ray/util/logging.h"

namespace ray {

//// Converters
// Converts C++ types to PyObject*.
// None of the converters hold the GIL, but all of them requires the GIL to be held.
// This means the caller should hold the GIL before calling these functions.
namespace {
class BytesConverter {
  // Specialization for types with a SerializeToString method
  template <typename Message>
  static std::string serialize(
      const Message &message,
      typename std::enable_if<std::is_member_function_pointer<
          decltype(&Message::SerializeToString)>::value>::type * = 0) {
    std::string result;
    if (message.SerializeToString(&result)) {
      return result;
    }
    return {};  // Return an empty string if serialization fails
  }

  // Specialization for types with a Binary method, i.e. `BaseID`s
  template <typename Message>
  static std::string serialize(const Message &message,
                               typename std::enable_if<std::is_member_function_pointer<
                                   decltype(&Message::Binary)>::value>::type * = 0) {
    return message.Binary();
  }

 public:
  template <typename T>
  static PyObject *convert(const T &arg) {
    std::string serialized_message = serialize(arg);
    return PyBytes_FromStringAndSize(serialized_message.data(),
                                     serialized_message.size());
  }

  static PyObject *convert(const std::string &arg) {
    return PyBytes_FromStringAndSize(arg.data(), arg.size());
  }
};

class BoolConverter {
 public:
  static PyObject *convert(bool arg) { return PyBool_FromLong(arg); }
};

class IntConverter {
 public:
  static PyObject *convert(int arg) { return PyLong_FromLong(arg); }
};

template <typename Inner>
class OptionalConverter {
 public:
  template <typename T>
  static PyObject *convert(const boost::optional<T> &arg) {
    if (arg) {
      return Inner::convert(*arg);
    } else {
      Py_RETURN_NONE;
    }
  }
};

template <typename Inner>
class VectorConverter {
 public:
  template <typename T>
  static PyObject *convert(const std::vector<T> &arg) {
    PyObject *list = PyList_New(arg.size());
    for (size_t i = 0; i < arg.size(); i++) {
      PyList_SetItem(list, i, Inner::convert(arg[i]));
    }
    return list;
  }
};

// Returns a Python tuple of two elements.
template <typename Left, typename Right>
class PairConverter {
 public:
  template <typename T, typename U>
  static PyObject *convert(const T &left, const U &right) {
    return PyTuple_Pack(2, Left::convert(left), Right::convert(right));
  }
};

template <typename KeyConverter, typename ValueConverter>
class MapConverter {
 public:
  template <typename MapType>
  static PyObject *convert(const MapType &arg) {
    PyObject *dict = PyDict_New();
    for (const auto &pair : arg) {
      PyObject *key = KeyConverter::convert(pair.first);
      PyObject *value = ValueConverter::convert(pair.second);
      PyDict_SetItem(dict, key, value);
      Py_XDECREF(key);
      Py_XDECREF(value);
    }
    return dict;
  }
};

// Converts ray::Status to Tuple[StatusCode, error_message, rpc_code]
class StatusConverter {
 public:
  static PyObject *convert(const ray::Status &status) {
    static_assert(std::is_same_v<char, std::underlying_type_t<ray::StatusCode>>,
                  "StatusCode underlying type should be char.");
    return PyTuple_Pack(3,
                        IntConverter::convert(static_cast<int>(status.code())),
                        BytesConverter::convert(status.message()),
                        IntConverter::convert(status.rpc_code()));
  }
};

using OptionalBytesConverter =
    PairConverter<StatusConverter, OptionalConverter<BytesConverter>>;
using OptionalIntConverter =
    PairConverter<StatusConverter, OptionalConverter<IntConverter>>;
using MultiBytesConverter =
    PairConverter<StatusConverter, VectorConverter<BytesConverter>>;
using PairBytesConverter = PairConverter<BytesConverter, BytesConverter>;

}  // namespace

// Wraps a Python `Callable[[T], None]` into a C++ `std::function<void(U)>`.
// This is a base class for all the callbacks, with subclass handling the conversion.
// The base class handles:
// - GIL management
// - PyObject Ref counting
// - Exception handling.
//
// `Converter`: a functor that converts U to owned PyObject*.
//
// TODO: For all these we also need to handle exc. We do this by giving the py_callable
// another arg so it becomes Callable[[T, Exception], None]. The exception is a
// 3-tuple of (type, value, tb).
// TODO: Subscribe need iterator/generator.
template <class Converter>
class PyCallback {
 private:
  class PythonGilHolder {
   public:
    PythonGilHolder() : state_(PyGILState_Ensure()) {}
    ~PythonGilHolder() { PyGILState_Release(state_); }

   private:
    PyGILState_STATE state_;
  };

 public:
  // Ref counted Python object.
  // This callback class is copyable, and as a std::function it's indeed copied around.
  // But we don't want to hold the GIL and do ref counting so many times, so we use a
  // shared_ptr to manage the ref count for us, and only call Py_XINCREF once.
  //
  // Note this ref counted is only from C++ side. If this ref count goes to 0, we only
  // Issue 1 decref, which does not necessarily free the object.
  std::shared_ptr<PyObject> py_callable;

  // Needed by Cython to place a placeholder object.
  PyCallback() {}

  PyCallback(PyObject *callable) {
    if (callable == nullptr) {
      py_callable = nullptr;
      return;
    }
    PythonGilHolder gil;
    Py_XINCREF(callable);

    py_callable = std::shared_ptr<PyObject>(callable, [](PyObject *obj) {
      PythonGilHolder gil;
      Py_XDECREF(obj);
    });
  }

  // Typically Args is just a single argument, but we allow multiple arguments for e.g.
  // (Status, boost::optional<MessageType>).
  template <typename... Args>
  void operator()(const Args &...args) {
    if (py_callable == nullptr) {
      return;
    }

    // Hold the GIL.
    PythonGilHolder gil;
    PyObject *arg_obj = Converter::convert(args...);
    PyObject *result = PyObject_CallFunctionObjArgs(py_callable.get(), arg_obj, NULL);
    Py_XDECREF(arg_obj);
    Py_XDECREF(result);
  }
};

// Concrete callback types.
// `(Status)` below means a 3-tuple of (StatusCode: int, error_message: bytes, rpc_code:
// int).
//
// For StatusCallback
// (Status) -> (Status)
using PyStatusCallback = PyCallback<StatusConverter>;
// For OptionalItemCallback<Serializable>
// (Status, boost::optional<T>) -> (Status, Optional[bytes])
using PyOptionalBytesCallback = PyCallback<OptionalBytesConverter>;
// (Status, boost::optional<int>) -> (Status, Optional[int])
using PyOptionalIntCallback = PyCallback<OptionalIntConverter>;
// For MultiItemCallback<Serializable>
// (Status, std::vector<T>) -> (Status, List[bytes])
using PyMultiBytesCallback = PyCallback<MultiBytesConverter>;
// For ItemCallback<Serializable>
// (T) -> (bytes)
using PyBytesCallback = PyCallback<BytesConverter>;
// For ItemCallback<std::unordered_map<Serializable, Serializable>
// (std::unordered_map<K, V>) -> (Dict[bytes, bytes])
using PyMapCallback = PyCallback<MapConverter<BytesConverter, BytesConverter>>;
// For SubscribeCallback<ID, Serializable>
// (ID, T) -> (bytes, bytes)
using PyPairBytesCallback = PyCallback<PairBytesConverter>;

}  // namespace ray
