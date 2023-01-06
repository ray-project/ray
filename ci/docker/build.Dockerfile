ARG DOCKER_IMAGE_BASE_BUILD
FROM $DOCKER_IMAGE_BASE_BUILD

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
RUN rm -rf /ray

RUN mkdir /ray
WORKDIR /ray

# Below should be re-run each time
COPY . .

RUN env

# init also calls install-dependencies.sh
RUN BUILD=1 bash --login -i ./ci/ci.sh init

# Set compiler here to build Ray with CLANG/LLVM
RUN export CC=clang CXX=clang++-12

RUN bash --login -i ./ci/ci.sh build

# Run determine test to run
RUN bash --login -i -c "python ./ci/pipeline/determine_tests_to_run.py --output=json > affected_set.json"
RUN cat affected_set.json

