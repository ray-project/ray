ARG DOCKER_IMAGE_BASE_TEST
FROM $DOCKER_IMAGE_BASE_TEST

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

RUN bash --login -i ./ci/env/install-dependencies.sh
