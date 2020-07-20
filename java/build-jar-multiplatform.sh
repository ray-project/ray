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
  local bazel_build=true
  if [[ $# = 2 ]]; then
    bazel_build=$2
  fi
  echo "Start building jar for $platform"
  JAR_DIR="$JAR_BASE_DIR"/$platform
  mkdir -p "$JAR_DIR"
  for p in "${JAVA_DIRS_PATH[@]}"; do
    cd "$WORKSPACE_DIR"/"$p"
    if [[ $bazel_build == "true" ]]; then
      echo "Starting building java native dependencies for $p"
      bazel build gen_maven_deps
      echo "Finished building java native dependencies for $p"
    fi
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
build_jars_darwin() {
  build_jars darwin
}

build_jars_multiplatform() {
  download_jars
  prepare_native
  build_jars multiplatform false
}

# download linux/darwin ray-runtime-$version.jar from s3
download_jars() {
   local version
   # ray jar version, ex: 0.1-SNAPSHORT
   version=$(python -c "import xml.etree.ElementTree as ET;  r = ET.parse('pom.xml').getroot(); print(r.find(r.tag.replace('project', 'version')).text);" | tail -n 1)
  wait_time=0
  sleep_time_units=1
  for os in 'darwin' 'linux'; do
    url=https://ray-wheels.s3-us-west-2.amazonaws.com/jars/"$TRAVIS_BRANCH/$TRAVIS_COMMIT/$os/ray-runtime-$version.jar"
    dest_file="$JAR_BASE_DIR/$os/ray-runtime-$version.jar"
    echo "Jar url: $url"
    echo "Jar dest_file: $dest_file"
    while true; do
      if ! wget -q "$url" -O "$dest_file">/dev/; then
        echo "Waiting $url to be ready for $wait_time seconds..."
        sleep $sleep_time_units
        wait_time=$((wait_time + sleep_time_units))
        if [[ wait_time == $((60 * 30)) ]]; then
          echo "Download $url timeout"
          exit -1
        fi
      else
        echo "Download $url to $dest_file succeed"
        break
      fi
    done
  done
}

# prepare native binaries and libraries.
prepare_native() {
  for os in 'darwin' 'linux'; do
    cd "$JAR_BASE_DIR"/"$os"
    jar xf "$(ls ray-runtime*.jar | grep -v sources | grep -v grep)" "$os"
    local native_dir="$WORKSPACE_DIR"/java/runtime/native_dependencies/native/"$os"
    mkdir -p "$native_dir"
    rm -rf "$native_dir"
    jar xf "$(ls ray-runtime*.jar | grep -v sources | grep -v grep)" "native/$os"
    mv "native/$os" "$native_dir"
    local native_dir="$WORKSPACE_DIR"/streaming/java/streaming-runtime/native_dependencies/native/"$os"
    mkdir -p "$native_dir"
    rm -rf "$native_dir"
    jar xf "$(ls streaming-runtime*.jar | grep -v sources | grep -v grep)" "native/$os"
    mv "native/$os" "$native_dir"
  done
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
  build_jars_linux
  ;;
darwin)
  build_jars_darwin
  ;;
multiplatform)
  build_jars_multiplatform
  ;;
deploy)
  deploy_jars
  ;;
*)
  echo "Execute command $1"
  $1
  ;;
esac

