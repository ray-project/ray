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

#include <string>
#include <vector>

#include "ray/util/logging.h"

namespace ray {
namespace gcs {

class PythonGilHolder {
 public:
  PythonGilHolder() : state_(PyGILState_Ensure()) {}
  ~PythonGilHolder() { PyGILState_Release(state_); }

 private:
  PyGILState_STATE state_;
};

// Facility for async bindings. C++ APIs need a callback (`std::function<void(T)>`),
// and in the callback we want to do type conversion and complete the Python future.
//
// An ideal API would be a `std::function<void(T)>` that wraps a Python callable, which
// holds references to a Python `Future`. However, Cython can't wrap a Python callable
// into a stateful C++ std::function. Instead we have to define Cython `cdef` functions
// who are translated to C++ functions, and use their function pointers.
//
// Because we can only work with stateless Cython functions, we need to keep the Future
// as a void* in this functor. This functor does not manage its lifetime: it assumes the
// void* is always valid. We Py_INCREF the Future in `incremented_fut` before passing it
// to PyCallback, and Py_DECREF it in `assign_and_decrement_fut` after the completion.
//
// Different APIs have different type signatures, but the code of completing the future
// is the same. So we ask 2 Cython function pointers: `Converter` and `Assigner`.
// `Converter` is unique for each API, converting C++ types to Python types.
// `Assigner` is shared by all APIs, completing the Python future.
//
// On C++ async API calling:
// 1. Create a Future.
// 2. Creates a `PyCallback` functor with `Converter` and `Assigner` and the Future.
// 3. Invokes the async API with the functor.
//
// On C++ async API completion:
// 1. The PyCallback functor is called. It acquires GIL and:
// 2. The functor calls the Cython function `Converter` with C++ types. It returns
//  `Tuple[result, exception]`.
// 3. The functor calls the Cython function `Assigner` with the tuple and the
//  Future (as void*). It assign the result or the exception to the Python future.
template <typename... Args>
class PyCallback {
 public:
  // The Converter is a Cython function that takes Args... and returns a PyObject*.
  // It must not raise exceptions.
  // The return PyObject* is passed to the Assigner.
  using Converter = PyObject *(*)(Args...);
  // It must not raise exceptions.
  using Assigner = void (*)(PyObject *, void *);

  PyCallback(Converter converter, Assigner assigner, void *context)
      : converter(converter), assigner(assigner), context(context) {}

  void operator()(Args &&...args) {
    PythonGilHolder gil;
    PyObject *result = converter(std::forward<Args>(args)...);
    CheckNoException();

    assigner(result, context);
    CheckNoException();
  }

  void CheckNoException() {
    if (PyErr_Occurred() != nullptr) {
      PyErr_Print();
      PyErr_Clear();
      RAY_LOG(FATAL) << "Python exception occurred in async binding code, exiting!";
    }
  }

 private:
  Converter converter = nullptr;
  Assigner assigner = nullptr;
  void *context = nullptr;
};

template <typename T>
using MultiItemPyCallback = PyCallback<Status, std::vector<T> &&>;

template <typename Data>
using OptionalItemPyCallback = PyCallback<Status, const std::optional<Data> &>;

using StatusPyCallback = PyCallback<Status>;

}  // namespace gcs

}  // namespace ray
