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

#include "ray/util/invoke_once_token.h"

#include "ray/util/env.h"

namespace ray {

void InvokeOnceToken::CheckInvokeOnce() {
  if (IsEnvTrue("RAY_DISABLE_INVOKE_ONCE_FOR_TEST")) {
    return;
  }
  bool expected = false;
  RAY_CHECK(invoked_.compare_exchange_strong(expected, /*desired=*/true))
      << "Invoke once token has been visited before.";
}

}  // namespace ray
