# syntax=docker/dockerfile:1.3-labs
# shellcheck disable=SC2148

ARG BASE_IMAGE
FROM "$BASE_IMAGE"

ENV REMOTE_BAZEL_CACHE="https://bazel-cache-dev.s3.us-west-2.amazonaws.com"

WORKDIR /tmp/ray
COPY . .

# setup bazel cache
RUN echo "build --config=ci" > ~/.bazelrc
RUN echo "build --announce_rc" >> ~/.bazelrc
RUN echo "build --remote_cache=${REMOTE_BAZEL_CACHE}" >> ~/.bazelrc

# build ray
RUN bazel build //:ray_pkg

# build dashboard
RUN cd dashboard/client && npm ci && npm run build

# install ray
RUN cd python/ && python3 -m pip install -v -e .

# install test dependencies
ARG TEST_ENVIRONMENT_SCRIPT

COPY "$TEST_ENVIRONMENT_SCRIPT" /tmp/post_build_script.sh
RUN /tmp/post_build_script.sh
