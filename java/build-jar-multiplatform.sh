#!/bin/bash

set -x

# Cause the script to exit if a single command fails.
set -e

FILE_DIR="$(cd "$(dirname "${BASH_SOURCE:-$0}")"; pwd)"
WORKSPACE_DIR="${FILE_DIR}/.."
JAVA_DIRS_PATH=('java' 'streaming/java')
RAY_JAVA_MODULES=('api' 'runtime')
RAY_STREAMING_JAVA_MODULES=('streaming-api' 'streaming-runtime' 'streaming-state')
JAR_BASE_DIR="$WORKSPACE_DIR"/.jar

build_jars() {
  local platform=$1
  echo "Start building jar for $platform"
  JAR_DIR="$JAR_BASE_DIR"/$platform
  mkdir -p "$JAR_DIR"
  for p in "${JAVA_DIRS_PATH[@]}"; do
    cd "$WORKSPACE_DIR"/"$p"
    echo "Starting building java native dependencies for $p"
    bazel build gen_maven_deps
    echo "Finished building java native dependencies for $p"
    echo "Start building jars for $p"
    mvn -T16 clean package install -Dmaven.test.skip=true -Dcheckstyle.skip
    mvn -T16 source:jar -Dmaven.test.skip=true -Dcheckstyle.skip
    echo "Finished building jars for $p"
  done
  copy_jars "$JAR_DIR"
  # ray runtime jar and streaming runtime jar are in a dir specifed by maven-jar-plugin
  cp -f "$WORKSPACE_DIR"/build/java/ray*.jar "$JAR_DIR"
  cp -f "$WORKSPACE_DIR"/streaming/build/java/streaming*.jar "$JAR_DIR"
  echo "Finished building jar for $platform"
}

copy_jars() {
  JAR_DIR=$1
  echo "Copy to dir $JAR_DIR"
  for module in "${RAY_JAVA_MODULES[@]}"; do
    cp -f "$WORKSPACE_DIR"/java/"$module"/target/*jar "$JAR_DIR"
  done
  for module in "${RAY_STREAMING_JAVA_MODULES[@]}"; do
    cp -f "$WORKSPACE_DIR"/streaming/java/"$module"/target/*jar "$JAR_DIR"
  done
  # ray runtime jar and streaming runtime jar are in a dir specifed by maven-jar-plugin
  cp -f "$WORKSPACE_DIR"/build/java/ray*.jar "$JAR_DIR"
  cp -f "$WORKSPACE_DIR"/streaming/build/java/streaming*.jar "$JAR_DIR"
}

# This function should be run in a docker container. See `build_jars_multiplatform`
build_jars_linux() {
  . "${WORKSPACE_DIR}"/ci/travis/install-dependencies.sh
  build_jars linux
}

# This function assuem all dependencies are installed already.
build_jars_macos() {
  build_jars macos
}

build_jars_multiplatform() {
  build_jars_macos

  # The -f flag is passed twice to also run git clean in the arrow subdirectory.
  # The -d flag removes directories. The -x flag ignores the .gitignore file,
  # and the -e flag ensures that we don't remove the .jar directory.
#  cd "$WORKSPACE_DIR" && git clean -f -f -x -d -e .jar && cd -
#  docker run -e --rm -w /ray -v "$WORKSPACE_DIR":/ray -ti maven:3.6-adoptopenjdk-8 /ray/java/build-jar-multiplatform.sh linux

  prepare_native
  build_jars multiplatform
}

# prepare native binaries and libraries.
prepare_native() {
  for os in 'macos' 'linux'; do
    cd "$JAR_BASE_DIR"/"$os"
    jar xf "$(ls ray-runtime*.jar | grep -v sources | grep -v grep)" "$os"
    local native_dir="$WORKSPACE_DIR"/java/runtime/native_dependencies
    mkdir -p "$native_dir"
    rm -rf "$native_dir"
    mv "$os" "$native_dir"
    jar xf "$(ls streaming-runtime*.jar | grep -v sources | grep -v grep)" "$os"
    local native_dir="$WORKSPACE_DIR"/streaming/java/streaming-runtime/native_dependencies
    mkdir -p "$native_dir"
    rm -rf "$native_dir"
    mv "$os" "$native_dir"
  done

  mkdir -p "$WORKSPACE_DIR"/java/runtime/native_dependencies
  cd "$WORKSPACE_DIR"/java/runtime/native_dependencies
  rm -rf *
  echo "$JAR_BASE_DIR"/macos/
  jar xf "$JAR_BASE_DIR"/linux/"$(ls ray-runtime*.jar | grep -v sources | grep -v grep)" linux
  jar xf "$JAR_BASE_DIR"/macos/"$(ls ray-runtime*.jar | grep -v sources | grep -v grep)" macos

  cd "$WORKSPACE_DIR"/streaming/java/streaming-runtime/native_dependencies
  rm -rf *
  jar xf "$JAR_BASE_DIR"/linux/"$(ls streaming-runtime*.jar | grep -v sources | grep -v grep)" linux
  jar xf "$JAR_BASE_DIR"/macos/"$(ls streaming-runtime*.jar | grep -v sources | grep -v grep)" macos
}

# This function assuem all multiplatform binaries are prepared already.
deploy_jars() {
  echo "Start deploying jars"
  cd "$WORKSPACE_DIR"/java
  mvn -T16 deploy -Dmaven.test.skip=true -Dcheckstyle.skip
  cd "$WORKSPACE_DIR"/streaming/java
  mvn -T16 deploy -Dmaven.test.skip=true -Dcheckstyle.skip
  echo "Finished deploying jars"
}

case $1 in
linux)
  build_jar_linux
  ;;
macos)
  build_jar_macos
  ;;
multiplatform)
  build_jars_multiplatform
  ;;
deploy)
  deploy_jars
  ;;
*)
  echo "ERROR: unknown option \"$1\", please pass linux/macos/multiplatform"
  echo
  exit -1
  ;;
esac

