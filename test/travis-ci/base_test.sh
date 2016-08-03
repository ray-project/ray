#!/bin/bash

if [[ $TRAVIS_OS_NAME == 'linux' ]]; then
  # Linux test uses Docker
  docker run amplab/ray:test-base bash -c 'source setup-env.sh && cd test && python runtest.py && python array_test.py && python microbenchmarks.py'
else
  # Mac OS X test
  source setup-env.sh
  pushd test
    runtest.py
    python array_test.py
    python microbenchmarks.py
  popd
fi
