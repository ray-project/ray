# The stress_test Docker image build a self-contained Ray instance for launching Ray.

FROM ray-project/base-deps

# We install ray and boto3 to enable the ray autoscaler as
# a test runner.
RUN conda install -y numpy
RUN pip install -U https://ray-wheels.s3-us-west-2.amazonaws.com/latest/ray-0.8.0.dev3-cp36-cp36m-manylinux1_x86_64.whl boto3
# We install this after the latest wheels -- this should not override the latest wheels.
RUN apt-get install -y zlib1g-dev
# The following is needed to support TensorFlow 1.14
RUN conda remove -y --force wrapt
RUN pip install gym[atari]==0.10.11 opencv-python-headless tensorflow lz4 keras pytest-timeout smart_open
RUN pip install --upgrade bayesian-optimization
RUN pip install --upgrade git+git://github.com/hyperopt/hyperopt.git
RUN pip install --upgrade sigopt nevergrad scikit-optimize hpbandster ConfigSpace lightgbm xgboost
RUN pip install -U mlflow
RUN pip install -U pytest-remotedata>=0.3.1
RUN conda install pytorch-cpu torchvision-cpu -c pytorch

# RUN mkdir -p /root/.ssh/

# We port the source code in so that we run the most up-to-date stress tests.
ADD ray.tar /ray
ADD git-rev /ray/git-rev
RUN python /ray/python/ray/setup-dev.py --yes

WORKDIR /ray
