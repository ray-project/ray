python train.py --env Walker2d-v1 --alg PolicyGradient --s3-bucket s3://rllib
python train.py --env PongNoFrameskip-v0 --alg DQN --s3-bucket s3://rllib
python train.py --env PongDeterministic-v0 --alg A3C --s3-bucket s3://rllib
python train.py --env Humanoid-v1 --alg EvolutionStrategies --s3-bucket s3://rllib
