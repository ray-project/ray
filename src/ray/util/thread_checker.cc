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

#include "ray/util/thread_checker.h"

namespace ray {

bool ThreadChecker::IsOnSameThread() const {
  const auto cur_id = std::this_thread::get_id();
  std::thread::id uninitialized_id;
  return thread_id_.compare_exchange_strong(uninitialized_id, cur_id) ||
         (uninitialized_id == cur_id);
}

}  // namespace ray
