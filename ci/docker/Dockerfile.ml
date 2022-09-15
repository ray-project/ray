FROM DOCKER_IMAGE_TEST

RLLIB_TESTING=1 TRAIN_TESTING=1 TUNE_TESTING=1 bash --login -i ./ci/env/install-dependencies.sh
