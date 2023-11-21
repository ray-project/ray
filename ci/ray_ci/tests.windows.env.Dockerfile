FROM rayproject/buildenv:windows

ENV BUILDKITE=true
ENV BUILDKITE_BAZEL_CACHE_URL=https://bazel-cache-dev.s3.us-west-2.amazonaws.com
ENV BUILD=1
ENV CI=true
ENV DL=1
ENV PYTHON=3.8
ENV RAY_USE_RANDOM_PORTS=1
ENV RAY_DEFAULT_BUILD=1
ENV RAY_INSTALL_JAVA=0
ENV RAY_ENABLE_WINDOWS_OR_OSX_CLUSTER=1
ENV LC_ALL=en_US.UTF-8
ENV LANG=en_US.UTF-8

COPY . .
RUN git config --global core.symlinks true
RUN git config --global core.autocrlf false
RUN mkdir /rayci
RUN git clone . /rayci
WORKDIR /rayci

RUN powershell ci/pipeline/fix-windows-container-networking.ps1
RUN powershell ci/pipeline/fix-windows-bazel.ps1
RUN conda init
RUN ./ci/ci.sh init
RUN ./ci/ci.sh build
