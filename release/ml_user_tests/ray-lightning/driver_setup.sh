#!/bin/bash

cd "${0%/*}" || exit 1

# Install extras (e.g. gym[atari]) are not allowed in constraint files.
# Hack to remove the first line and all brackets in `requirements_tune.txt`
# so that requirements_tune.txt can be used as a constraint in driver_requirements.txt
sed '1d' ./../../../python/requirements/ml/requirements_tune.txt | sed 's/\[//g;s/\]//g' > tune_constraints.txt

pip install -U -r ./driver_requirements.txt