// Copyright 2021 The Ray Authors.
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

// This header file contains Ray internal flags that should not be set
// by users. They are set by Ray with environment variable
// RAY_{VARIABLE NAME}.
//
// The format is used to avoid code duplication.
// It can be included multiple times in ray_config.h, and each inclusion
// could use a different definition of the RAY_INTERNAL_FLAG macro.
// Macro definition format: RAY_INTERNAL_FLAG(type, name, default_value).
// NOTE: This file should NOT be included in any file other than ray_config.h.

/// Ray Job ID.
RAY_INTERNAL_FLAG(std::string, JOB_ID, "")

/// Raylet process ID.
RAY_INTERNAL_FLAG(std::string, RAYLET_PID, "")

/// Override the random node ID for testing.
RAY_INTERNAL_FLAG(std::string, OVERRIDE_NODE_ID_FOR_TESTING, "")
