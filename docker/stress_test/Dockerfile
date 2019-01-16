# The stress_test Docker image build a self-contained Ray instance for launching Ray.

FROM ray-project/base-deps
RUN pip install -U pandas
RUN pip install -U https://s3-us-west-2.amazonaws.com/ray-wheels/latest/ray-0.6.1-cp27-cp27mu-manylinux1_x86_64.whl
RUN pip install -U boto3
ADD ray.tar /ray
ADD git-rev /ray/git-rev
# TODO(rliaw): Remove
WORKDIR /ray
