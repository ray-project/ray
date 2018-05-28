#!/usr/bin/env bash
ROOT_DIR=$(cd "$(dirname "${BASH_SOURCE:-$0}")"; pwd)
$ROOT_DIR/../build.sh -l java

pushd $ROOT_DIR/../thirdparty/build/arrow/java
mvn clean install -pl plasma -am -Dmaven.test.skip
popd
pushd $ROOT_DIR/../java
mvn clean install -Dmaven.test.skip
mvn test
popd