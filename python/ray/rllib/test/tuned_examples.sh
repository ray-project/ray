#!/bin/bash

python train.py --env Hopper-v1 --config '{"gamma": 0.995, "kl_coeff": 1.0, "num_sgd_iter": 20, "sgd_stepsize": 1e-4, "sgd_batchsize": 32768, "devices": ["/gpu:0", "/gpu:1", "/gpu:2", "/gpu:3"], "tf_session_args": {"device_count": {"GPU": 4}, "log_device_placement": false, "allow_soft_placement": true}, "timesteps_per_batch": 160000, "num_workers": 64}' --alg PPO --upload-dir s3://bucketname/

python train.py --env CartPole-v1 --config '{"kl_coeff": 1.0, "num_sgd_iter": 20, "sgd_stepsize": 1e-4, "sgd_batchsize": 32768, "devices": ["/gpu:0", "/gpu:1", "/gpu:2", "/gpu:3"], "tf_session_args": {"device_count": {"GPU": 4}, "log_device_placement": false, "allow_soft_placement": true}, "timesteps_per_batch": 160000, "num_workers": 64}' --alg PPO --upload-dir s3://bucketname/

python train.py --env Walker2d-v1 --config '{"kl_coeff": 1.0, "num_sgd_iter": 20, "sgd_stepsize": 1e-4, "sgd_batchsize": 32768, "devices": ["/gpu:0", "/gpu:1", "/gpu:2", "/gpu:3"], "tf_session_args": {"device_count": {"GPU": 4}, "log_device_placement": false, "allow_soft_placement": true}, "timesteps_per_batch": 320000, "num_workers": 64}' --alg PPO --upload-dir s3://bucketname/

python train.py --env Humanoid-v1 --config '{"kl_coeff": 1.0, "num_sgd_iter": 20, "sgd_stepsize": 1e-4, "sgd_batchsize": 32768, "devices": ["/gpu:0", "/gpu:1", "/gpu:2", "/gpu:3"], "tf_session_args": {"device_count": {"GPU": 4}, "log_device_placement": false, "allow_soft_placement": true}, "timesteps_per_batch": 320000, "num_workers": 64, "model": {"free_log_std": true}, "use_gae": false}' --alg PPO --upload-dir s3://bucketname/

python train.py --env Humanoid-v1 --config '{"lambda": 0.95, "clip_param": 0.2, "kl_coeff": 1.0, "num_sgd_iter": 20, "sgd_stepsize": 1e-4, "sgd_batchsize": 32768, "horizon": 5000, "devices": ["/gpu:0", "/gpu:1", "/gpu:2", "/gpu:3"], "tf_session_args": {"device_count": {"GPU": 4}, "log_device_placement": false, "allow_soft_placement": true}, "timesteps_per_batch": 320000, "num_workers": 64, "model": {"free_log_std": true}, "write_logs": false}' --alg PPO --upload-dir s3://bucketname/

python train.py --env PongNoFrameskip-v0 --alg DQN --upload-dir s3://bucketname/

python train.py --env PongDeterministic-v4 --alg A3C --config '{"num_workers": 16, "num_batches_per_iteration": 1000, "batch_size": 20}' --upload-dir s3://bucketname/

python train.py --env Humanoid-v1 --alg EvolutionStrategies --upload-dir s3://bucketname/
