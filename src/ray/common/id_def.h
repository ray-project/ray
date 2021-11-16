// Copyright 2017 The Ray Authors.
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

// This header file is used to avoid code duplication.
// It can be included multiple times in id.h, and each inclusion
// could use a different definition of the DEFINE_UNIQUE_ID macro.
// Macro definition format: DEFINE_UNIQUE_ID(id_type).
// NOTE: This file should NOT be included in any file other than id.h.

DEFINE_UNIQUE_ID(FunctionID)
DEFINE_UNIQUE_ID(ActorClassID)
DEFINE_UNIQUE_ID(WorkerID)
DEFINE_UNIQUE_ID(ConfigID)
DEFINE_UNIQUE_ID(NodeID)
