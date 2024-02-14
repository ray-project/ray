#!/bin/bash
# This script is used to build an extra layer on top of the base anyscale/ray image 
# to run the agent stress test.

set -exo pipefail

pip install -U "git+https://github.com/ray-project/xgboost_ray.git"