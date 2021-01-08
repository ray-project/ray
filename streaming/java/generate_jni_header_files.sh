#!/usr/bin/env bash

set -e
set -x

cd "$(dirname "$0")"

bazel build all_streaming_tests_deploy.jar

function generate_one()
{
  file=${1//./_}.h
  javah -classpath ../../bazel-bin/streaming/java/all_streaming_tests_deploy.jar "$1"

  # prepend licence first
  cat <<EOF > ../src/lib/java/"$file"
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
  # then append the generated header file
  cat "$file" >> ../src/lib/java/"$file"
  rm -f "$file"
}

generate_one io.ray.streaming.runtime.transfer.channel.ChannelId
generate_one io.ray.streaming.runtime.transfer.DataReader
generate_one io.ray.streaming.runtime.transfer.DataWriter
generate_one io.ray.streaming.runtime.transfer.TransferHandler

rm -f io_ray_streaming_*.h
