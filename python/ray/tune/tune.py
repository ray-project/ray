#!/usr/bin/env python

"""Command-line tool for tuning hyperparameters with Ray.

MNIST tuning example:
    ./tune.py -f examples/tune_mnist_ray.yaml
"""

from ray.rllib import train
import sys

# TODO(ekl) right now this is a thin wrapper around the rllib training script,
# however in the future we should have a separate command line tool here.
train.main(sys.argv[1:] + ['--alg=script'])
