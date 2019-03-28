# The deploy Docker image build a self-contained Ray instance suitable
# for end users.

FROM ray-project/base-deps
ADD ray.tar /ray
ADD git-rev /ray/git-rev
RUN /ray/ci/travis/install-bazel.sh
ENV PATH=$PATH:/root/bin
WORKDIR /ray/python
RUN pip install -e .
WORKDIR /ray
