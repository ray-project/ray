#!/bin/bash

# Hyperparameter optimization test
# Runs only on Docker under Linux
if [[ $TRAVIS_OS_NAME == 'linux' ]]; then
  docker run --shm-size=500m amplab/ray:test-examples bash -c 'source setup-env.sh && cd examples/hyperopt && python driver.py'
fi
