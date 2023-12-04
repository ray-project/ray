FROM rayproject/buildenv:windows

ENV BUILDKITE_BAZEL_CACHE_URL=https://bazel-cache-dev.s3.us-west-2.amazonaws.com
ENV PYTHON=3.8
ENV RAY_USE_RANDOM_PORTS=1
ENV RAY_DEFAULT_BUILD=1
ENV RAY_INSTALL_JAVA=0
ENV RAY_ENABLE_WINDOWS_OR_OSX_CLUSTER=1
ENV LC_ALL=en_US.UTF-8
ENV LANG=en_US.UTF-8

COPY . .
RUN bash ci/ray_ci/build_base_windows.sh

WORKDIR /rayci
