#!/bin/bash

set -exo pipefail

pip3 install --no-cache-dir mlflow==2.19.0 scikit-learn==1.6.0 xgboost>=3.0.0
