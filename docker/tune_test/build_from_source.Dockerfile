# The stress_test Docker image build a self-contained Ray instance for launching Ray.

FROM ray-project/base-deps

RUN pip install -U pip

# We install this after the latest wheels -- this should not override the latest wheels.
# Needed to run Tune example with a 'plot' call - which does not actually render a plot, but throws an error.
RUN pip install torch==1.4.0+cpu torchvision==0.5.0+cpu -f https://download.pytorch.org/whl/torch_stable.html
RUN pip install https://storage.googleapis.com/tensorflow/linux/cpu/tensorflow_cpu-2.1.0-cp36-cp36m-manylinux2010_x86_64.whl

COPY requirements.txt .
RUN pip install -r requirements.txt

# We port the source code in so that we run the most up-to-date stress tests.
ADD ray.tar /ray
ADD git-rev /ray/git-rev

RUN bash /ray/ci/travis/install-bazel.sh --system
RUN echo 'build --remote_cache="https://storage.googleapis.com/ray-bazel-cache"' >> $HOME/.bazelrc
RUN echo 'build --remote_upload_local_results=false' >> $HOME/.bazelrc
RUN cd /ray/python; pip install -e . --verbose

WORKDIR /ray
