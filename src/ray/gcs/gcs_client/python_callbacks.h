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
// However, Cython can't wrap a Python callable to a stateful C++ std::function.
//
// Fortunately, Cython can convert *pure* Cython functions to C++ function pointers.
// Hence we make this C++ Functor `CpsHandler` to wrap Python calls to C++ callbacks.
//
// Different APIs have different type signatures, but the code of completing the future
// is the same. So we ask 2 Cython function pointers: `Handler` and `Continuation`.
// `Handler` is unique for each API, converting C++ types to Python types. `Continuation`
// is shared by all APIs, completing the Python future.
//
// One issue is the `Continuation` have to be stateless, so we need to keep the Future
// in the functor. But we don't want to expose the Future to C++ too much, so we keep
// it as a void*. For that, we Py_INCREF the Future in the functor and Py_DECREF it
// after the completion.
//
// On C++ async API calling:
// 1. Create a Future.
// 2. Creates a `CpsHandler` functor with `Handler` and `Continuation` and the Future.
// 3. Invokes the async API with the functor.
//
// On C++ async API completion:
// 1. The CpsHandler functor is called. It acquires GIL and:
// 2. The functor calls the Cython function `Handler` with C++ types. It returns
//  `Tuple[result, exception]`.
// 3. The functor calls the Cython function `Continuation` with the tuple and the
//  Future (as void*). It completes the Python future with the result or with the
//  exception.
template <typename... Args>
class CpsHandler {
 public:
  // The Handler is a Cython function that takes Args... and returns a PyObject*.
  // It must not raise exceptions.
  // The return PyObject* is passed to the Continuation.
  using Handler = PyObject *(*)(Args...);
  // It must not raise exceptions.
  using Continuation = void (*)(PyObject *, void *);

  CpsHandler(Handler handler, Continuation continuation, void *context)
      : handler(handler), continuation(continuation), context(context) {}

  void operator()(Args &&...args) {
    PythonGilHolder gil;
    PyObject *result = handler(std::forward<Args>(args)...);
    continuation(result, context);
  }

 private:
  Handler handler = nullptr;
  Continuation continuation = nullptr;
  void *context = nullptr;
};

template <typename T>
using MultiItemCpsHandler = CpsHandler<Status, std::vector<T> &&>;

template <typename Data>
using OptionalItemCpsHandler = CpsHandler<Status, const std::optional<Data> &>;

}  // namespace gcs

}  // namespace ray
