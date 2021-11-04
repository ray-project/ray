#!/bin/bash

# Hack to remove brackets, so that requirements_tune.txt can be used as a constraint in driver_requirements.txt
sed '1d' ./../../../python/requirements/ml/requirements_tune.txt | sed 's/\[//g;s/\]//g' > tune_constraints.txt

pip install -U -r ./driver_requirements.txt