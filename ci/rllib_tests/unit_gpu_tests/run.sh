#!/usr/bin/env bash

# Run all test cases, but with a forced num_gpus=1.
# TODO: (sven) chose correct dir and run over all RLlib tests and example scripts!
source activate tensorflow_p36 && export RAY_FORCE_NUM_GPUS=1 && cd ~ && python -m pytest test_attention_net_learning.py
