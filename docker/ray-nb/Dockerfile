ARG GPU
ARG DOCKER_PREFIX

# We will steal our cross-built wheel from here
FROM "$DOCKER_PREFIX"ray-wheel:nightly"$GPU" AS raywheelsrc

RUN echo "hi"

# This is our actual image that we use as the base
FROM holdenk/jupyter-hub-magicsingleuser-sample:0.10.2-n412.h25a21283
# Specify bazel (see https://github.com/bazelbuild/bazel/issues/12887)
ENV USE_BAZEL_VERSION=3.7.2

USER root

RUN apt-get update \
    && apt-get install -yq graphviz git build-essential \
    && apt-get install -y curl unzip cmake gcc g++ build-essential libunwind8 libunwind-dev libunwind8-dev openjdk-11-jdk zlib1g-dev \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

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

RUN touch /hello_holden


USER $NB_USER

RUN conda install -c conda-forge --yes mamba
RUN mamba install --yes pyarrow
RUN (mamba install --yes py-spy) ||  \
    (pushd /tmp && curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs -o rustup.sh && chmod a+x rustup.sh && ./rustup.sh -y && source $HOME/.cargo/env && git clone https://github.com/benfred/py-spy && pushd py-spy && python setup.py install && popd && popd)
COPY --from=raywheelsrc /ray/python/dist/ray*.whl /tmp/
RUN pip --no-cache-dir install $(ls /tmp/ray*.whl)[all]


