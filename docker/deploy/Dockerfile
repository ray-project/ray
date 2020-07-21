# The deploy Docker image build a self-contained Ray instance suitable
# for end users.

FROM ray-project/base-deps
ADD ray.tar /ray
ADD git-rev /ray/git-rev
RUN cd /ray && git init && ./ci/travis/install-bazel.sh --system
ENV PATH=$PATH:/root/bin
RUN echo 'build --remote_cache="https://storage.googleapis.com/ray-bazel-cache"' >> $HOME/.bazelrc 
RUN echo 'build --remote_upload_local_results=false' >> $HOME/.bazelrc 
WORKDIR /ray/
# The result of bazel build is reused in pip install. It if run first to allow
# for failover to serial build if parallel build requires too much resources.
RUN bazel build //:ray_pkg || bazel build --jobs 1 //:ray_pkg
WORKDIR /ray/python/
RUN pip install -e .
WORKDIR /ray
