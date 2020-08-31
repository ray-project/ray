#!/usr/bin/env bash

set -e
set -x

cd "$(dirname "$0")"

(cd .. && bazel build //java:all_tests_deploy.jar)

function generate_one()
{
  file=${1//./_}.h
  javah -classpath ../bazel-bin/java/all_tests_deploy.jar "$1"
  clang-format -i "$file"

  cat <<EOF > ../src/ray/core_worker/lib/java/"$file"
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

EOF
  cat "$file" >> ../src/ray/core_worker/lib/java/"$file"
  rm -f "$file"
}

generate_one io.ray.runtime.RayNativeRuntime
generate_one io.ray.runtime.task.NativeTaskSubmitter
generate_one io.ray.runtime.context.NativeWorkerContext
generate_one io.ray.runtime.actor.NativeActorHandle
generate_one io.ray.runtime.object.NativeObjectStore
generate_one io.ray.runtime.task.NativeTaskExecutor
generate_one io.ray.runtime.gcs.GlobalStateAccessor
generate_one io.ray.runtime.metric.NativeMetric

# Remove empty files
rm -f io_ray_runtime_RayNativeRuntime_AsyncContext.h
rm -f io_ray_runtime_task_NativeTaskExecutor_NativeActorContext.h
