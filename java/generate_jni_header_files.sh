#!/usr/bin/env bash

set -e
set -x

cd "$(dirname "$0")"

(cd .. && bazel build //java:all_tests_deploy.jar)

function generate_one()
{
  file=${2//./_}.h
  javac -h ./ "$1" -classpath ../bazel-bin/java/all_tests_deploy.jar -d runtime/target/
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

generate_one runtime/src/main/java/io/ray/runtime/RayNativeRuntime.java io.ray.runtime.RayNativeRuntime
generate_one runtime/src/main/java/io/ray/runtime/task/NativeTaskSubmitter.java io.ray.runtime.task.NativeTaskSubmitter
generate_one runtime/src/main/java/io/ray/runtime/context/NativeWorkerContext.java io.ray.runtime.context.NativeWorkerContext
generate_one runtime/src/main/java/io/ray/runtime/actor/NativeActorHandle.java io.ray.runtime.actor.NativeActorHandle
generate_one runtime/src/main/java/io/ray/runtime/object/NativeObjectStore.java io.ray.runtime.object.NativeObjectStore
generate_one runtime/src/main/java/io/ray/runtime/gcs/GlobalStateAccessor.java io.ray.runtime.gcs.GlobalStateAccessor
generate_one runtime/src/main/java/io/ray/runtime/metric/NativeMetric.java io.ray.runtime.metric.NativeMetric

# Remove empty files
rm -f io_ray_runtime_RayNativeRuntime_AsyncContext.h
rm -f io_ray_runtime_task_NativeTaskExecutor_NativeActorContext.h
