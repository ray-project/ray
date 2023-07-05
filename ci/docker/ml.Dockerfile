ARG DOCKER_IMAGE_BASE_ML
FROM $DOCKER_IMAGE_BASE_ML

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

RUN RLLIB_TESTING=1 TRAIN_TESTING=1 TUNE_TESTING=1 bash --login -i ./ci/env/install-dependencies.sh

# Install Ray
RUN SKIP_BAZEL_BUILD=1 RAY_INSTALL_JAVA=0 bash --login -i -c -- "python3 -m pip install -e /ray/python/"
