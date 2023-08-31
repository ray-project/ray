ARG DOCKER_IMAGE_BASE_TEST=cr.ray.io/rayproject/oss-ci-base_test
FROM $DOCKER_IMAGE_BASE_TEST

COPY . .

RUN RLLIB_TESTING=1 TRAIN_TESTING=1 TUNE_TESTING=1 ./ci/env/install-dependencies.sh
