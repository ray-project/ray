# The stress_test Docker image build a self-contained Ray instance for launching Ray.

FROM ray-project/base-deps

# We install ray and boto3 to enable the ray autoscaler as
# a test runner.
RUN pip install -U pip
RUN pip install -U https://ray-wheels.s3-us-west-2.amazonaws.com/latest/ray-0.9.0.dev0-cp36-cp36m-manylinux1_x86_64.whl

# We install this after the latest wheels -- this should not override the latest wheels.
# Needed to run Tune example with a 'plot' call - which does not actually render a plot, but throws an error.
RUN pip install torch==1.4.0+cpu torchvision==0.5.0+cpu -f https://download.pytorch.org/whl/torch_stable.html
RUN pip install https://storage.googleapis.com/tensorflow/linux/cpu/tensorflow_cpu-2.1.0-cp36-cp36m-manylinux2010_x86_64.whl

COPY requirements.txt .
RUN pip install -r requirements.txt

# We port the source code in so that we run the most up-to-date stress tests.
ADD ray.tar /ray
ADD git-rev /ray/git-rev
RUN python /ray/python/ray/setup-dev.py --yes

WORKDIR /ray
