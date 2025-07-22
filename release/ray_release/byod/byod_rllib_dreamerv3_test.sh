#!/bin/bash
# This script is used to build an extra layer on top of the base anyscale/ray image
# to run RLlib dreamerv3 release tests.

set -exo pipefail

# Only DreamerV3 still uses tf on the new API stack. But requires tf==2.11.1 to run.
pip uninstall -y tensorflow tensorflow_probability
pip install tensorflow==2.11.1 tensorflow_probability==0.19.0
