#!/bin/bash

python train.py --env Walker2d-v1 --alg PolicyGradient --upload-dir s3://rllib
python train.py --env PongNoFrameskip-v0 --alg DQN --upload-dir s3://rllib
python train.py --env PongDeterministic-v0 --alg A3C --upload-dir s3://rllib
python train.py --env Humanoid-v1 --alg EvolutionStrategies --upload-dir s3://rllib
