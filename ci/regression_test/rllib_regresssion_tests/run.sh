#!/usr/bin/env bash

source activate tensorflow_p36 && pip install torch torchvision
source activate tensorflow_p36 && rllib train -f compact-regression-test.yaml
