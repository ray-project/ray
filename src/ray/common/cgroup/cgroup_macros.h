// Copyright 2025 The Ray Authors.
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

#include "ray/common/macros.h"
#include "ray/common/status.h"

#if defined(RAY_SCHECK_OK_CGROUP)
#error "RAY_SCHECK_OK_CGROUP is already defined."
#else
#define __RAY_SCHECK_OK_CGROUP(expr, boolname) \
  auto boolname = (expr);                      \
  if (!boolname) return Status(StatusCode::Invalid, /*msg=*/"", RAY_LOC())

// Invoke the given [expr] which returns a boolean convertible type; and return error
// status if Failed. Cgroup operations on filesystem are not expected to fail after
// precondition checked, so we use INVALID as the status code.
//
// Example usage:
// RAY_SCHECK_OK_CGROUP(DoSomething()) << "DoSomething Failed";
#define RAY_SCHECK_OK_CGROUP(expr) \
  __RAY_SCHECK_OK_CGROUP(expr, RAY_UNIQUE_VARIABLE(cgroup_op))
#endif
