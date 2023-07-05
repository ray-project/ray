ARG DOCKER_IMAGE_BASE_GPU
FROM $DOCKER_IMAGE_BASE_GPU

ARG BUILDKITE_PULL_REQUEST
ARG BUILDKITE_COMMIT
ARG BUILDKITE_PULL_REQUEST_BASE_BRANCH

ENV BUILDKITE_PULL_REQUEST=${BUILDKITE_PULL_REQUEST}
ENV BUILDKITE_COMMIT=${BUILDKITE_COMMIT}
ENV BUILDKITE_PULL_REQUEST_BASE_BRANCH=${BUILDKITE_PULL_REQUEST_BASE_BRANCH}
ENV TRAVIS_COMMIT=${BUILDKITE_COMMIT}

# Move out of working dir /ray
# Delete stale data
WORKDIR /
# Preserve requirements_compiled.txt
RUN mv /ray/python/requirements_compiled.txt /tmp/requirements_compiled.txt || true
RUN rm -rf /ray

RUN mkdir /ray
WORKDIR /ray

# Copy new ray files
COPY . .

RUN mv /tmp/requirements_compiled.txt /ray/python/requirements_compiled.txt || true

RUN env

RUN RLLIB_TESTING=1 TRAIN_TESTING=1 TUNE_TESTING=1 bash --login -i ./ci/env/install-dependencies.sh

# Install Ray
RUN SKIP_BAZEL_BUILD=1 RAY_INSTALL_JAVA=0 bash --login -i -c -- "python3 -m pip install -e /ray/python/"
