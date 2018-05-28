#!/usr/bin/env bash

pushd ../thirdparty/build/arrow/java/plasma
mvn clean install -Dmaven.test.skip
popd
mvn clean install -Dmaven.test.skip