# The stress_test Docker image build a self-contained Ray instance for launching Ray.

FROM ray-project/base-deps

# We install ray and boto3 to enable the ray autoscaler as
# a test runner.
RUN conda install -y numpy
RUN pip install -U pip
RUN pip install -U https://ray-wheels.s3-us-west-2.amazonaws.com/latest/ray-0.9.0.dev0-cp36-cp36m-manylinux1_x86_64.whl
RUN pip install -U boto3
# We install this after the latest wheels -- this should not override the latest wheels.
# Needed to run Tune example with a 'plot' call - which does not actually render a plot, but throws an error.
RUN apt-get install -y zlib1g-dev libgl1-mesa-dev
# The following is needed to support TensorFlow 1.14
RUN conda remove -y --force wrapt
RUN pip install gym[atari] opencv-python-headless tensorflow lz4 pytest-timeout smart_open tensorflow_probability dm_tree
RUN pip install -U h5py  # Mutes FutureWarnings
RUN pip install --upgrade bayesian-optimization hyperopt
RUN pip install ConfigSpace==0.4.10
RUN pip install --upgrade sigopt nevergrad scikit-optimize hpbandster lightgbm xgboost torch torchvision tensorboardX dragonfly-opt
RUN pip install -U tabulate mlflow
RUN pip install -U pytest-remotedata>=0.3.1

# RUN mkdir -p /root/.ssh/

# We port the source code in so that we run the most up-to-date stress tests.
ADD ray.tar /ray
ADD git-rev /ray/git-rev
RUN python /ray/python/ray/setup-dev.py --yes

WORKDIR /ray
