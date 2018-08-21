#!/bin/bash
scripts_dir=$(cd "$(dirname "${BASH_SOURCE:-$0}")"; pwd)
cd $scripts_dir

export RAY_CONFIG=$scripts_dir/ray/ray.config.ini
export LD_LIBRARY_PATH=$scripts_dir/ray/native/lib:$LD_LIBRARY_PATH
java -ea -classpath ray/java/lib/*:ray/java/lib/commons-cli-1.3.1.jar org.ray.cli.RayCli "$@"

