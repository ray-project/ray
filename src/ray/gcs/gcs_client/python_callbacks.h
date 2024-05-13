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

#include "ray/util/logging.h"

namespace ray {
namespace {

// Specialization for types with a SerializeToString method
template <typename Message>
std::string serialize(const Message &message,
                      typename std::enable_if<std::is_member_function_pointer<
                          decltype(&Message::SerializeToString)>::value>::type * = 0) {
  std::string result;
  if (message.SerializeToString(&result)) {
    return result;
  }
  return {};  // Return an empty string if serialization fails
}

// Specialization for types with a Binary method
template <typename Message>
std::string serialize(
    const Message &message,
    typename std::enable_if<
        std::is_member_function_pointer<decltype(&Message::Binary)>::value>::type * = 0) {
  return message.Binary();
}

}  // namespace

// Wraps a Python `Callable[[bytes], None]` into a C++ `std::function<void(T)>`.
// It serializes the T, assumed to be a protobuf message type,
// to std::string and passes it to the Python callable.
//
// The serialization supports Protobuf and BaseID.
//
// TODO: handle exceptions by asking for another exc callback.
// TODO: handle non-single-obj cases:
// - Empty (void)
// - Status (Status)
// - OptionalItem (Status, boost::optional<T>)
// - MultiItem (Status, std::vector<T>)
// - Subscribe (ID, T)
// - Item (T) <- you are here
// - Map (absl::flat_hash_map<K, V>)
//
// For all these we also need to handle exc.
// TODO: is there a not-so-boilerplate way to do it? e.g. python tuple
//
// TODO: Subscribe need iterator/generator.
template <class Message>
class PyStringCallback {
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
  PyStringCallback() {}

  PyStringCallback(PyObject *callable) {
    if (callable == nullptr) {
      py_callable = nullptr;
      return;
    }
    PythonGilHolder gil;
    Py_XINCREF(callable);
    RAY_LOG(ERROR) << "incref callable to " << Py_REFCNT(callable);

    py_callable = std::shared_ptr<PyObject>(callable, [](PyObject *obj) {
      PythonGilHolder gil;
      Py_XDECREF(obj);
      RAY_LOG(ERROR) << "decref callable to " << Py_REFCNT(obj);
    });
  }

  void operator()(const Message &message) {
    RAY_LOG(ERROR) << "Calling Python callback with message. " << py_callable.get();
    if (py_callable == nullptr) {
      return;
    }

    std::string serialized_message = serialize(message);

    RAY_LOG(ERROR) << "Calling Python callback with message of size "
                   << serialized_message.size() << " bytes.";

    // Hold the GIL.
    PythonGilHolder gil;
    RAY_LOG(ERROR) << "Held GIL.";

    // RAY_LOG(ERROR) << py_callable.get() << " callable "
    //                << PyCallable_Check(py_callable.get());
    // PyObject *name = PyObject_GetAttrString(py_callable, "__name__");
    // RAY_LOG(ERROR) << "callable name " << PyUnicode_AsUTF8(name);

    PyObject *arg =
        PyBytes_FromStringAndSize(serialized_message.data(), serialized_message.size());
    PyObject *result = PyObject_CallFunctionObjArgs(py_callable.get(), arg, NULL);
    RAY_LOG(ERROR) << "Called Python callback. ret = " << result;
    if (PyErr_Occurred()) {
      PyErr_Print();  // This will print the error to stderr and clear the error
                      // indicator.
    }
    Py_XDECREF(arg);
    Py_XDECREF(result);
  }
};

}  // namespace ray