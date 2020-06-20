# The stress_test Docker image build a self-contained Ray instance for launching Ray.

FROM ray-project/base-deps

# We install ray and boto3 to enable the ray autoscaler as
# a test runner.
RUN pip install -U https://ray-wheels.s3-us-west-2.amazonaws.com/latest/ray-0.9.0.dev0-cp36-cp36m-manylinux1_x86_64.whl boto3
RUN mkdir -p /root/.ssh/

# We port the source code in so that we run the most up-to-date stress tests.
ADD ray.tar /ray
ADD git-rev /ray/git-rev
WORKDIR /ray
