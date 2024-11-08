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

// Utils to optimize branch prediction.

#pragma once

#include "absl/base/optimization.h"

namespace ray {

// Instruct the compiler to predict that `value` will be true with high
// probability.
inline constexpr bool Likely(bool value) { return ABSL_PREDICT_TRUE(value); }

// Instruct the compiler to predict that `value` will be false with high
// probability.
inline constexpr bool Unlikely(bool value) { return ABSL_PREDICT_FALSE(value); }

}  // namespace ray
