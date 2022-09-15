FROM [Dockerfile.test image]

RLLIB_TESTING=1 TRAIN_TESTING=1 TUNE_TESTING=1 bash --login -i ./ci/env/install-dependencies.sh
