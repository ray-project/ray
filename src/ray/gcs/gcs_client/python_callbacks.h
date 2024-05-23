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

#include <boost/optional/optional.hpp>
#include <functional>
#include <string>
#include <type_traits>
#include <vector>

#include "absl/container/flat_hash_map.h"
#include "ray/common/status.h"
#include "ray/util/logging.h"

namespace ray {

//// Converters
// Converts C++ types to PyObject*.
// None of the converters hold the GIL, but all of them requires the GIL to be held.
// This means the caller should hold the GIL before calling these functions.
//
// A Converter is a functor that takes a C++ type and returns a PyObject*.
// The conversion may fail, in which case the converter should set an exception and return
// nullptr.
//
// By default you can use `DefaultConverter::convert` to convert any type. If you need
// special handling you can compose out your own.
class BytesConverter {
  // Serializes the message to a string. Returns false if the serialization fails.
  // template <typename T>
  // static bool serialize(const T &message, std::string &result);

  // Specialization for types with a SerializeToString method
  template <typename Message>
  static bool serialize(const Message &message,
                        std::string &result,
                        typename std::enable_if<std::is_member_function_pointer<
                            decltype(&Message::SerializeToString)>::value>::type * = 0) {
    return message.SerializeToString(&result);
  }

  // Specialization for types with a Binary method, i.e. `BaseID`s
  // Never fails.
  template <typename ID>
  static bool serialize(
      const ID &id,
      std::string &result,
      typename std::enable_if<
          std::is_member_function_pointer<decltype(&ID::Binary)>::value>::type * = 0) {
    result = id.Binary();
    return true;
  }

 public:
  template <typename T>
  static PyObject *convert(const T &arg) {
    std::string serialized_message;
    if (serialize(arg, serialized_message)) {
      return PyBytes_FromStringAndSize(serialized_message.data(),
                                       serialized_message.size());
    } else {
      PyErr_SetString(PyExc_ValueError, "Failed to serialize message.");
      Py_RETURN_NONE;
    }
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
      PyObject *item = Inner::convert(arg[i]);
      if (item == nullptr) {
        // Failed to convert an item. Free the list and all items within.
        Py_DECREF(list);
        return nullptr;
      }
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
    PyObject *result = PyTuple_New(2);
    PyObject *left_obj = Left::convert(left);
    if (left_obj == nullptr) {
      Py_DECREF(result);
      return nullptr;
    }
    PyTuple_SetItem(result, 0, left_obj);
    PyObject *right_obj = Right::convert(right);
    if (right_obj == nullptr) {
      Py_DECREF(result);
      return nullptr;
    }
    PyTuple_SetItem(result, 1, right_obj);
    return result;
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
      if (key == nullptr) {
        Py_DECREF(dict);
        return nullptr;
      }
      PyObject *value = ValueConverter::convert(pair.second);
      if (value == nullptr) {
        Py_DECREF(key);
        Py_DECREF(dict);
        return nullptr;
      }
      if (PyDict_SetItem(dict, key, value) < 0) {
        Py_DECREF(key);
        Py_DECREF(value);
        Py_DECREF(dict);
        return nullptr;
      }
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

// Default converter, converts all types implemented above.
// Resolution:
// - single bool, int, status: BoolConverter, IntConverter, StatusConverter
// - optional<T>:              OptionalConverter<T>
// - vector<T>:                VectorConverter<T>
// - single generic argument:  BytesConverter
// - 2 args (T, U):            PairConverter<T, U>
// - map<K, V>:                MapConverter<K, V> (we can't do generics over MapType for
// collision w/ BytesConverter, so we specialize for std::unordered_map and
// absl::flat_hash_map)
class DefaultConverter {
 public:
  static PyObject *convert(bool arg) { return BoolConverter::convert(arg); }
  static PyObject *convert(int arg) { return IntConverter::convert(arg); }
  static PyObject *convert(const Status &arg) { return StatusConverter::convert(arg); }

  template <typename T>
  static PyObject *convert(const T &arg) {
    return BytesConverter::convert(arg);
  }
  template <typename T>
  static PyObject *convert(const boost::optional<T> &arg) {
    return OptionalConverter<DefaultConverter>::convert(arg);
  }
  template <typename T>
  static PyObject *convert(const std::vector<T> &arg) {
    return VectorConverter<DefaultConverter>::convert(arg);
  }
  template <typename T, typename U>
  static PyObject *convert(const T &left, const U &right) {
    return PairConverter<DefaultConverter, DefaultConverter>::convert(left, right);
  }
  template <typename Key, typename Value>
  static PyObject *convert(const std::unordered_map<Key, Value> &arg) {
    return MapConverter<DefaultConverter, DefaultConverter>::convert(arg);
  }
  template <typename Key, typename Value>
  static PyObject *convert(const absl::flat_hash_map<Key, Value> &arg) {
    return MapConverter<DefaultConverter, DefaultConverter>::convert(arg);
  }
};

// Wraps a Python `Callable[[T, Exception], None]` into a C++ `std::function<void(U)>`.
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
  //
  // Converts the arguments to a Python Object and may raise exceptions.
  // Invokes the Python callable with (converted_arg, None), or (None, exception).
  //
  // The callback should not return anything and should not raise exceptions. If it
  // raises, we catch it and log it.
  template <typename... Args>
  void operator()(const Args &...args) {
    if (py_callable == nullptr) {
      return;
    }

    // Hold the GIL.
    PythonGilHolder gil;
    PyObject *arg_obj = Converter::convert(args...);
    if (arg_obj == nullptr) {
      // Failed to convert the arguments. The exception is set by the converter.
      PyObject *exc_obj = PyErr_Occurred();
      if (exc_obj == nullptr) {
        // The converter didn't set an exception?
        RAY_LOG(WARNING) << "Failed to convert arguments, but no exception set.";
        PyErr_SetString(PyExc_ValueError, "Failed to convert arguments.");
        exc_obj = PyErr_Occurred();
      }
      PyObject *result =
          PyObject_CallFunctionObjArgs(py_callable.get(), Py_None, exc_obj, NULL);
      Py_XDECREF(result);
    } else {
      PyObject *result =
          PyObject_CallFunctionObjArgs(py_callable.get(), arg_obj, Py_None, NULL);
      Py_XDECREF(arg_obj);
      Py_XDECREF(result);
    }
    if (PyErr_Occurred()) {
      // Our binding code raised exceptions. Not much we can do here. Print it out.
      PyObject *ptype, *pvalue, *ptraceback;
      PyErr_Fetch(&ptype, &pvalue, &ptraceback);
      PyErr_NormalizeException(&ptype, &pvalue, &ptraceback);
      PyObject *str_exc = PyObject_Str(pvalue);
      const char *exc_str = PyUnicode_AsUTF8(str_exc);

      RAY_LOG(ERROR) << "Python exception in cpython binding callback: " << exc_str;

      // Clean up
      Py_XDECREF(ptype);
      Py_XDECREF(pvalue);
      Py_XDECREF(ptraceback);
      Py_XDECREF(str_exc);

      PyErr_Clear();
    }
  }
};

// Concrete callback types.
// Most types are using the DefaultConverter, but we allow specialization for some types.
using PyDefaultCallback = PyCallback<DefaultConverter>;
// Specialization for a pair of (Status, Vector<T>).
// We need this because `MultiItemCallback` uses (Status, std::vector<T>&&) not const ref.
// and C++ can't deduce it well (will implicit convert the vector&& to bool)
template <typename ItemConverter>
using PyMultiItemCallback =
    PyCallback<PairConverter<StatusConverter, VectorConverter<ItemConverter>>>;

}  // namespace ray
