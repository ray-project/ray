ARG DOCKER_IMAGE_BASE_ML
FROM $DOCKER_IMAGE_BASE_ML

# Move out of working dir /ray
# Delete stale data
WORKDIR /
RUN rm -rf /ray

RUN mkdir /ray
WORKDIR /ray

# Copy new ray files
COPY . .

# Install Ray
RUN SKIP_BAZEL_BUILD=1 RAY_INSTALL_JAVA=0 bash --login -i -c -- "python3 -m pip install -e /ray/python/"

RUN RLLIB_TESTING=1 TRAIN_TESTING=1 TUNE_TESTING=1 bash --login -i ./ci/env/install-dependencies.sh
