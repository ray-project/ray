#!/usr/bin/env bash

# Cause the script to exit if a single command fails.
set -e
# Show explicitly which commands are currently running.
set -x

ROOT_DIR=$(cd "$(dirname "${BASH_SOURCE:-$0}")"; pwd)

if [ ! -z "$RAY_USE_CMAKE" ] ; then
  pushd $ROOT_DIR/../java
  mvn clean install -Dmaven.test.skip
  check_style=$(mvn checkstyle:check)
  echo "${check_style}"
  [[ ${check_style} =~ "BUILD FAILURE" ]] && exit 1
  
  # test raylet
  mvn test | tee mvn_test
  if [ `grep -c "BUILD FAILURE" mvn_test` -eq '0' ]; then
      rm mvn_test
      echo "Tests passed under CLUSTER mode!"
  else
      rm mvn_test
      exit 1
  fi
  # test raylet under SINGLE_PROCESS mode
  mvn test -Dray.run-mode=SINGLE_PROCESS | tee dev_mvn_test
  if [ `grep -c "BUILD FAILURE" dev_mvn_test` -eq '0' ]; then
      rm dev_mvn_test
      echo "Tests passed under SINGLE_PROCESS mode!"
  else
      rm dev_mvn_test
      exit 1
  fi
else
  pushd $ROOT_DIR/..
  bazel build -c opt //java:all
  pushd $ROOT_DIR/../java/test
  java -jar -Dray.home=$ROOT_DIR/../ -Djava.library.path=$ROOT_DIR/../bazel-bin:$ROOT_DIR/../bazel-bin/externl/plasma:$ROOT_DIR/../bazel-genfiles $ROOT_DIR/../bazel-bin/java/AllTests_deploy.jar $ROOT_DIR/../java/testng.xml

  java -jar -Dray.home=$ROOT_DIR/../ -Dray.run-mode=SINGLE_PROCESS -Djava.library.path=$ROOT_DIR/../bazel-bin:$ROOT_DIR/../bazel-bin/externl/plasma:$ROOT_DIR/../bazel-genfiles $ROOT_DIR/../bazel-bin/java/AllTests_deploy.jar $ROOT_DIR/../java/testng.xml
fi

popd
