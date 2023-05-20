#!/bin/bash

set -x

# Cause the script to exit if a single command fails.
set -e

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE:-$0}")"; pwd)"
WORKSPACE_DIR="${ROOT_DIR}/.."
JAVA_DIRS_PATH=('java')
RAY_JAVA_MODULES=('api' 'runtime' 'serve')
JAR_BASE_DIR="$WORKSPACE_DIR"/.jar
mkdir -p "$JAR_BASE_DIR"
cd "$WORKSPACE_DIR/java"
# ray jar version, ex: 0.1-SNAPSHORT
version=$(python -c "import xml.etree.ElementTree as ET;  r = ET.parse('pom.xml').getroot(); print(r.find(r.tag.replace('project', 'version')).text);" | tail -n 1)
cd -

check_java_version() {
  local VERSION
  VERSION=$(java  -version 2>&1 | awk -F '"' '/version/ {print $2}')
  if [[ ! $VERSION =~ 1.8 ]]; then
    echo "Java version is $VERSION. Please install jkd8."
    exit 1
  fi
}

build_jars() {
  local platform="$1"
  local bazel_build="${2:-true}"
  echo "bazel_build $bazel_build"
  echo "Start building jar for $platform"
  local JAR_DIR="$JAR_BASE_DIR/$platform"
  mkdir -p "$JAR_DIR"
  for p in "${JAVA_DIRS_PATH[@]}"; do
    cd "$WORKSPACE_DIR/$p"
    bazel build cp_java_generated
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
  # ray runtime jar is in a dir specifed by maven-jar-plugin
  cp -f "$WORKSPACE_DIR"/build/java/ray*.jar "$JAR_DIR"
  echo "Finished building jar for $platform"
}

copy_jars() {
  local JAR_DIR="$1"
  echo "Copy to dir $JAR_DIR"
  for module in "${RAY_JAVA_MODULES[@]}"; do
    cp -f "$WORKSPACE_DIR"/java/"$module"/target/*jar "$JAR_DIR"
  done
  # ray runtime jar is in a dir specifed by maven-jar-plugin
  cp -f "$WORKSPACE_DIR"/build/java/ray*.jar "$JAR_DIR"
}

# This function assuem all dependencies are installed already.
build_jars_linux() {
  build_jars linux
}

# This function assuem all dependencies are installed already.
build_jars_darwin() {
  build_jars darwin
}

build_jars_multiplatform() {
  if [ "${TRAVIS-}" = true ]; then
    if [[ "${TRAVIS_REPO_SLUG-}" != "ray-project/ray" || "${TRAVIS_PULL_REQUEST-}" != "false" ]]; then
      echo "Skip build multiplatform jars when this build is from a pull request or
        not a build for commit in ray-project/ray."
      return
    fi
  fi
  if download_jars "ray-runtime-$version.jar"; then
    prepare_native
    build_jars multiplatform false
  else
    echo "download_jars failed, skip building multiplatform jars"
  fi
}

# Download darwin/windows ray-related jar from s3
# This function assumes linux jars exist already.
download_jars() {
  local wait_time=0
  local sleep_time_units=60

  for f in "$@"; do
    for os in 'darwin' 'linux' 'windows'; do
      if [[ "$os" == "windows" ]]; then
        continue
      fi
      local url="https://ray-wheels.s3-us-west-2.amazonaws.com/jars/$TRAVIS_BRANCH/$TRAVIS_COMMIT/$os/$f"
      mkdir -p "$JAR_BASE_DIR/$os"
      local dest_file="$JAR_BASE_DIR/$os/$f"
      echo "Jar url: $url"
      echo "Jar dest_file: $dest_file"
      while true; do
        if ! wget -q "$url" -O "$dest_file">/dev/null; then
          echo "Waiting $url to be ready for $wait_time seconds..."
          sleep $sleep_time_units
          wait_time=$((wait_time + sleep_time_units))
          if [[ wait_time == $((sleep_time_units * 100)) ]]; then
            echo "Download $url timeout"
            return 1
          fi
        else
          echo "Download $url to $dest_file succeed"
          break
        fi
      done
    done
  done
  echo "Download jars took $wait_time seconds"
}

# prepare native binaries and libraries.
prepare_native() {
  for os in 'darwin' 'linux'; do
    cd "$JAR_BASE_DIR/$os"
    jar xf "ray-runtime-$version.jar" "native/$os"
    local native_dir="$WORKSPACE_DIR/java/runtime/native_dependencies/native/$os"
    mkdir -p "$native_dir"
    rm -rf "$native_dir"
    mv "native/$os" "$native_dir"
  done
}

# Return 0 if native bianries and libraries exist and 1 if not.
native_files_exist() {
  local os
  for os in 'darwin' 'linux'; do
    native_dirs=()
    native_dirs+=("$WORKSPACE_DIR/java/runtime/native_dependencies/native/$os")
    for native_dir in "${native_dirs[@]}"; do
      if [ ! -d "$native_dir" ]; then
        echo "$native_dir doesn't exist"
        return 1
      fi
    done
  done
}

# This function assume all multiplatform binaries are prepared already.
deploy_jars() {
  if [ "${TRAVIS-}" = true ]; then
    mkdir -p ~/.m2
    echo "<settings><servers><server><id>ossrh</id><username>${OSSRH_KEY}</username><password>${OSSRH_TOKEN}</password></server></servers></settings>" > ~/.m2/settings.xml
    if [[ "$TRAVIS_REPO_SLUG" != "ray-project/ray" ||
     "$TRAVIS_PULL_REQUEST" != "false" || "$TRAVIS_BRANCH" != "master" ]]; then
      echo "Skip deploying jars when this build is from a pull request or
      not a build for commit of master branch in ray-project/ray"
      return
    fi
  fi
  echo "Start deploying jars"
  if native_files_exist; then
    (
      cd "$WORKSPACE_DIR/java"
      mvn -T16 install deploy -Dmaven.test.skip=true -Dcheckstyle.skip -Prelease -Dgpg.skip="${GPG_SKIP:-true}"
    )
    echo "Finished deploying jars"
  else
    echo "Native bianries/libraries are not ready, skip deploying jars."
  fi
}

if [ -z "${BUILDKITE-}" ]; then
  check_java_version
fi

case "$1" in
linux) # build jars that only contains Linux binaries.
  build_jars_linux
  ;;
darwin) # build jars that only contains macos binaries.
  build_jars_darwin
  ;;
multiplatform) # downloading jars of multiple platforms and packaging them into one jar.
  build_jars_multiplatform
  ;;
deploy) # Deploy jars to maven repository.
  deploy_jars
  ;;
*)
  echo "Execute command $*"
  "$@"
  ;;
esac
