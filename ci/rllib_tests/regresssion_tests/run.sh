#!/usr/bin/env bash

source activate tensorflow_p36 && rllib train -f compact-regression-test.yaml
