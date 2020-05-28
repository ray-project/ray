# The deploy Docker image build a self-contained Ray instance suitable
# for end users.

FROM ray-project/base-deps
ADD ray.tar /ray
ADD git-rev /ray/git-rev
RUN git init && /ray/ci/travis/install-bazel.sh
ENV PATH=$PATH:/root/bin
WORKDIR /ray/
RUN bazel build //:ray_pkg || bazel build --jobs 1 //:ray_pkg
WORKDIR /ray/python/
RUN pip install -e .
WORKDIR /ray
