#!/bin/bash

# L-BFGS Test
# Runs only on Docker under Linux
if [[ $TRAVIS_OS_NAME == 'linux' ]]; then
  docker run --shm-size=500m amplab/ray:test-examples bash -c 'source setup-env.sh && cd examples/lbfgs && python driver.py'
fi
