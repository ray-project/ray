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
// It can be included multiple times in ray_config.h, and each inclusion
// could use a different definition of the RAY_CONFIG macro.
// Macro definition format: RAY_CONFIG(type, name, default_value).
// NOTE: This file should NOT be included in any file other than ray_config.h.

/// The interval to replenish node instances of all the virtual clusters.
RAY_CONFIG(uint64_t, node_instances_replenish_interval_ms, 30000)

/// The interval to check and delete expired job clusters.
RAY_CONFIG(uint64_t, expired_job_clusters_gc_interval_ms, 30000)

// The interval of the delayed local node cleanup. When a node is removed out
// of the virtual cluster, the cleanup function can be delayed. It allows other handlers
// executed first, and leaves some time for tasks to finish (before force killing them).
RAY_CONFIG(uint64_t, local_node_cleanup_delay_interval_ms, 30000)
