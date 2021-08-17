ARG GPU
ARG DOCKER_PREFIX="rayproject/"

# We will steal our cross-built wheel from here
FROM "$DOCKER_PREFIX"ray-wheel:nightly"$GPU" AS raywheel
WORKDIR /ray

# This is our actual image that we use as the base
FROM "$DOCKER_PREFIX"base-notebook:nightly

RUN conda install -c conda-forge --yes mamba
# For Click
ENV LC_ALL=C.UTF-8
ENV LANG=C.UTF-8
# Specify bazel (see https://github.com/bazelbuild/bazel/issues/12887)
ENV USE_BAZEL_VERSION=3.7.2
USER root
RUN apt-get update && sudo apt-get install -y curl bash
USER $nbuser
RUN echo "export PATH=~/anaconda3/bin/:~/.cargo/bin:\$PATH" >> ~/.bashrc
SHELL ["/bin/bash", "-c"]
RUN export PATH=$HOME/.cargo/bin:$HOME/anaconda3/bin/:$PATH
# Install py-spy no arm on conda yet so fall through to pypi wheel
RUN pip install --upgrade pip
RUN (mamba install --yes py-spy) || (pip install py-spy)
# Install deps
# For Click
ENV LC_ALL=C.UTF-8
ENV LANG=C.UTF-8
COPY requirements.txt .
# Use multiple cores to compile the C code :)
ENV MAKEFLAGS -j 4

RUN apt-get update -y \
    && apt-get install -y tzdata bash libhdf5-dev pkg-config \
    && (apt-get install -y libhdf5-100 || apt-get install -y libhdf5-103) \
    && apt-get install -y gcc g++ make cmake wget \
    && rm -rf /var/lib/apt/lists/* \
    && apt-get clean

RUN mamba install --yes scipy pandas numba scikit-learn numpy
# Install the generic requirements.
RUN  mamba install --yes pyarrow
# Install bazelisk, this is needed for dm_tree on ARM since there is no wheel.
RUN arch=$(uname -m); \
    echo "Arch is $arch"; \
    if [ "$arch" == "aarch64" ]; then \
	BAZEL=bazelisk-linux-arm64; \
    elif [ "$arch" == "x86_64" ]; then \
	BAZEL=bazelisk-linux-amd64; \
    fi; \
    wget -q https://github.com/bazelbuild/bazelisk/releases/download/v1.7.5/${BAZEL} -O /tmp/${BAZEL}; \
    chmod a+x /tmp/${BAZEL};\
    sudo mv /tmp/${BAZEL} /usr/bin/bazel; \
    export PATH=$PATH:/usr/bin;
# Install the opencv as a seperate step for caching happiness
RUN pip install opencv-python-headless
RUN pip install tensorflow==2.5.1 || (pip install h5py && pip install https://github.com/bitsy-ai/tensorflow-arm-bin/releases/download/v2.4.0/tensorflow-2.4.0-cp37-none-linux_aarch64.whl)
RUN pip install -r requirements.txt --extra-index-url=https://www.piwheels.org/simple

# Install ray
RUN mkdir -p /tmp/ray-wheels
COPY --from=raywheel /ray/python/dist/*.whl /tmp/ray-wheels
RUN pip install tensorflow==2.5.1 || pip install https://github.com/bitsy-ai/tensorflow-arm-bin/releases/download/v2.4.0/tensorflow-2.4.0-cp37-none-linux_aarch64.whl
RUN pip --no-cache-dir install --no-index --find-links="/tmp/ray-wheels/" ray[default]
