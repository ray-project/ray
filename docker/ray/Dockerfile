ARG GPU
ARG DOCKER_PREFIX

#tag::bzl[]
# Specify bazel (see https://github.com/bazelbuild/bazel/issues/12887)
ENV USE_BAZEL_VERSION=3.7.2
#end::bzl[]

# Do a seperate build stage so we don't have to ship all of build dependencies
FROM "$DOCKER_PREFIX"ray-deps:nightly"$GPU" AS builder
# For Click
ENV LC_ALL=C.UTF-8
ENV LANG=C.UTF-8

ADD ray.tar /ray
ADD git-rev /ray/git-rev
# Install dependencies needed to build ray
USER root
RUN apt-get update && apt-get install -y curl unzip cmake gcc g++ build-essential libunwind8 libunwind-dev libunwind8-dev openjdk-11-jdk && apt-get clean
#RUN cd /ray && git init && ./ci/travis/install-bazel.sh --system
ENV PATH=$PATH:/home/ray/bin
RUN echo 'build --remote_cache="https://storage.googleapis.com/ray-bazel-cache"' >> $HOME/.bazelrc
RUN echo 'build --remote_upload_local_results=false' >> $HOME/.bazelrc
WORKDIR /ray/
# The result of bazel build is reused in pip install. It if run first to allow
# for failover to serial build if parallel build requires too much resources.
RUN bazel build //:ray_pkg || bazel build --jobs 1 //:ray_pkg
WORKDIR /ray/python/
RUN python3 setup.py bdist_wheel
#RUN pip install -e .
WORKDIR /ray


FROM "$DOCKER_PREFIX"ray-deps:nightly"$GPU"
WORKDIR /ray
# For Click
ENV LC_ALL=C.UTF-8
ENV LANG=C.UTF-8
# py-spy on arm is happier with rust install as per https://gist.github.com/heavyinfo/aa0bf2feb02aedb3b38eef203b4b8cc4
RUN sudo apt-get update && sudo apt-get install -y curl bash
RUN echo "export PATH=~/anaconda3/bin/:~/.cargo/bin:\$PATH" >> ~/.bashrc
SHELL ["/bin/bash", "-c"]
# Install py-spy, it runs out of process but has difficulty installing in ARM. To allow it to be installed from source on ARM we need to install cargo.
RUN export PATH=$HOME/.cargo/bin:$HOME/anaconda3/bin/:$PATH
RUN (mamba install --yes py-spy) ||  \
    (pushd /tmp && curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs -o rustup.sh && chmod a+x rustup.sh && ./rustup.sh -y && source $HOME/.cargo/env && git clone https://github.com/benfred/py-spy && pushd py-spy && python setup.py install && popd && popd)
COPY --from=builder /ray/python/dist/ray*.whl .
RUN pip --no-cache-dir install $(ls ray*.whl)[all]
