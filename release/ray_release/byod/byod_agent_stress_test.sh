#!/bin/bash
# This script is used to build an extra layer on top of the base anyscale/ray image 
# to run the agent stress test.

set -exo pipefail

pip install https://ray-ci-deps-wheels.s3.us-west-2.amazonaws.com/AutoROM.accept_rom_license-0.5.4-py3-none-any.whl
