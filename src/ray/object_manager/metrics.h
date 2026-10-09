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

#include "ray/stats/metric.h"
#include "ray/util/size_literals.h"

namespace ray {

using ray::literals::operator""_MiB;

inline ray::stats::Histogram GetObjectStoreDistHistogramMetric() {
  return ray::stats::Histogram{
      /*name=*/"object_store_dist",
      /*description=*/"The distribution of object size in bytes",
      /*unit=*/"MiB",
      /*boundaries=*/
      {32_MiB,
       64_MiB,
       128_MiB,
       256_MiB,
       512_MiB,
       1024_MiB,
       2048_MiB,
       4096_MiB,
       8192_MiB,
       16384_MiB},
      /*tag_keys=*/{"Source"},
  };
}

inline ray::stats::Gauge GetObjectStoreAvailableMemoryGaugeMetric() {
  return ray::stats::Gauge(
      /*name=*/"object_store_available_memory",
      /*description=*/"Amount of memory currently available in the object store.",
      /*unit=*/"bytes");
}

inline ray::stats::Gauge GetObjectStoreUsedMemoryGaugeMetric() {
  return ray::stats::Gauge(
      /*name=*/"object_store_used_memory",
      /*description=*/"Amount of memory currently occupied in the object store.",
      /*unit=*/"bytes");
}

inline ray::stats::Gauge GetObjectStoreFallbackMemoryGaugeMetric() {
  return ray::stats::Gauge(
      /*name=*/"object_store_fallback_memory",
      /*description=*/"Amount of memory in fallback allocations in the filesystem.",
      /*unit=*/"bytes");
}

inline ray::stats::Gauge GetObjectStoreLocalObjectsGaugeMetric() {
  return ray::stats::Gauge(
      /*name=*/"object_store_num_local_objects",
      /*description=*/"Number of objects currently in the object store.",
      /*unit=*/"objects");
}

inline ray::stats::Gauge GetObjectManagerPullRequestsGaugeMetric() {
  return ray::stats::Gauge(
      /*name=*/"object_manager_num_pull_requests",
      /*description=*/"Number of active pull requests for objects.",
      /*unit=*/"requests");
}

inline ray::stats::Gauge GetObjectManagerBytesGaugeMetric() {
  return ray::stats::Gauge(
      /*name=*/"object_manager_bytes",
      /*description=*/
      "Number of bytes pushed or received by type {PushedFromLocalPlasma, "
      "PushedFromLocalDisk, Received}.",
      /*unit=*/"bytes",
      /*tag_keys=*/{"Type"});
}

inline ray::stats::Gauge GetObjectManagerReceivedChunksGaugeMetric() {
  return ray::stats::Gauge(
      /*name=*/"object_manager_received_chunks",
      /*description=*/
      "Number object chunks received broken per type {Total, FailedTotal, "
      "FailedCancelled, FailedPlasmaFull}.",
      /*unit=*/"chunks",
      /*tag_keys=*/{"Type"});
}

inline ray::stats::Gauge GetPullManagerUsageBytesGaugeMetric() {
  return ray::stats::Gauge(
      /*name=*/"pull_manager_usage_bytes",
      /*description=*/
      "The total number of bytes usage broken per type {Available, BeingPulled, Pinned}",
      /*unit=*/"bytes",
      /*tag_keys=*/{"Type"});
}

inline ray::stats::Gauge GetPullManagerRequestedBundlesGaugeMetric() {
  return ray::stats::Gauge(
      /*name=*/"pull_manager_requested_bundles",
      /*description=*/
      "Number of requested bundles broken per type {Get, Wait, TaskArgs}.",
      /*unit=*/"bundles",
      /*tag_keys=*/{"Type"});
}

inline ray::stats::Gauge GetPullManagerRequestsGaugeMetric() {
  return ray::stats::Gauge(
      /*name=*/"pull_manager_requests",
      /*description=*/"Number of pull requests broken per type {Queued, Active, Pinned}.",
      /*unit=*/"requests",
      /*tag_keys=*/{"Type"});
}

inline ray::stats::Gauge GetPullManagerActiveBundlesGaugeMetric() {
  return ray::stats::Gauge(
      /*name=*/"pull_manager_active_bundles",
      /*description=*/"Number of active bundle requests",
      /*unit=*/"bundles",
      /*tag_keys=*/{"Type"});
}

inline ray::stats::Gauge GetPullManagerRetriesTotalGaugeMetric() {
  return ray::stats::Gauge(
      /*name=*/"pull_manager_retries_total",
      /*description=*/"Number of cumulative pull retries.",
      /*unit=*/"retries",
      /*tag_keys=*/{"Type"});
}

inline ray::stats::Gauge GetPullManagerNumObjectPinsGaugeMetric() {
  return ray::stats::Gauge(
      /*name=*/"pull_manager_num_object_pins",
      /*description=*/
      "Number of object pin attempts by the pull manager, can be {Success, Failure}.",
      /*unit=*/"pins",
      /*tag_keys=*/{"Type"});
}

inline ray::stats::Histogram GetPullManagerObjectRequestTimeMsHistogramMetric() {
  return ray::stats::Histogram(
      /*name=*/"pull_manager_object_request_time_ms",
      /*description=*/
      "Time between initial object pull request and local pinning of the object. ",
      /*unit=*/"ms",
      /*boundaries=*/{1, 10, 100, 1000, 10000},
      /*tag_keys=*/{"Type"});
}

inline ray::stats::Gauge GetPushManagerNumPushesRemainingGaugeMetric() {
  return ray::stats::Gauge(
      /*name=*/"push_manager_num_pushes_remaining",
      /*description=*/"Number of pushes not completed.",
      /*unit=*/"pushes",
      /*tag_keys=*/{"Type"});
}

inline ray::stats::Gauge GetPushManagerChunksGaugeMetric() {
  return ray::stats::Gauge(
      /*name=*/"push_manager_chunks",
      /*description=*/
      "Number of object chunks transfer broken per type {InFlight, Remaining}.",
      /*unit=*/"chunks",
      /*tag_keys=*/{"Type"});
}

}  // namespace ray
