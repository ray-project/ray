# The development Docker image build a self-contained Ray instance suitable
# for developers that need the source code to actively modify.

FROM rayproject/ray-deps:latest
ADD ray.tar /ray
ADD git-rev /ray/git-rev
# Install dependencies needed to build ray
RUN sudo apt-get update && sudo apt-get install -y curl unzip cmake gcc g++ && sudo apt-get clean
RUN sudo chown -R ray:users /ray && cd /ray && git init && ./ci/env/install-bazel.sh --system
ENV PATH=$PATH:/home/ray/bin
RUN echo 'build --remote_cache="https://storage.googleapis.com/ray-bazel-cache"' >> $HOME/.bazelrc 
RUN echo 'build --remote_upload_local_results=false' >> $HOME/.bazelrc 
WORKDIR /ray/
# The result of bazel build is reused in pip install. It if run first to allow
# for failover to serial build if parallel build requires too much resources.
RUN bazel build //:ray_pkg || bazel build --jobs 1 //:ray_pkg
WORKDIR /ray/python/
RUN pip install -e .
WORKDIR /ray
