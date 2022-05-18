# The base-deps Docker image installs main libraries needed to run Ray

# The GPU options are NVIDIA CUDA developer images.
ARG BASE_IMAGE="ubuntu:focal"
FROM ${BASE_IMAGE}
# FROM directive resets ARG
ARG BASE_IMAGE
# If this arg is not "autoscaler" then no autoscaler requirements will be included
ARG AUTOSCALER="autoscaler"
ENV TZ=America/Los_Angeles
# TODO(ilr) $HOME seems to point to result in "" instead of "/home/ray"
ENV PATH "/home/ray/anaconda3/bin:$PATH"
ARG DEBIAN_FRONTEND=noninteractive
ARG PYTHON_VERSION=3.7.7

ARG RAY_UID=1000
ARG RAY_GID=100

RUN apt-get update -y \
    && apt-get install -y sudo tzdata \
    && useradd -ms /bin/bash -d /home/ray ray --uid $RAY_UID --gid $RAY_GID \
    && usermod -aG sudo ray \
    && echo 'ray ALL=NOPASSWD: ALL' >> /etc/sudoers \
    && rm -rf /var/lib/apt/lists/* \
    && apt-get clean

USER $RAY_UID
ENV HOME=/home/ray

RUN sudo apt-get update -y && sudo apt-get upgrade -y \
    && sudo apt-get install -y \
        git \
        wget \
        cmake \
        g++ \ 
        zlib1g-dev \
        $(if [ "$AUTOSCALER" = "autoscaler" ]; then echo \
        tmux \
        screen \
        rsync \
        openssh-client \
        gnupg; fi) \
    && wget \
        --quiet "https://repo.anaconda.com/miniconda/Miniconda3-latest-Linux-x86_64.sh" \
        -O /tmp/miniconda.sh \
    && /bin/bash /tmp/miniconda.sh -b -u -p $HOME/anaconda3 \
    && $HOME/anaconda3/bin/conda init \ 
    && echo 'export PATH=$HOME/anaconda3/bin:$PATH' >> /home/ray/.bashrc \
    && rm /tmp/miniconda.sh \
    && $HOME/anaconda3/bin/conda install -y \
        libgcc python=$PYTHON_VERSION \
    && $HOME/anaconda3/bin/conda clean -y --all \
    && $HOME/anaconda3/bin/pip install --no-cache-dir \
        flatbuffers \
        cython==0.29.26 \
        # Necessary for Dataset to work properly.
        numpy\>=1.20 \
        psutil \
    # To avoid the following error on Jenkins:
    # AttributeError: 'numpy.ufunc' object has no attribute '__module__'
    && $HOME/anaconda3/bin/pip uninstall -y dask \ 
    # We install cmake temporarily to get psutil
    && sudo apt-get autoremove -y cmake zlib1g-dev \
        # We keep g++ on GPU images, because uninstalling removes CUDA Devel tooling
        $(if [ "$BASE_IMAGE" = "ubuntu:focal" ]; then echo \
        g++; fi) \
    # Either install kubectl or remove wget 
    && (if [ "$AUTOSCALER" = "autoscaler" ]; \
        then wget -O - -q https://packages.cloud.google.com/apt/doc/apt-key.gpg | sudo apt-key add - \
        && sudo touch /etc/apt/sources.list.d/kubernetes.list \
        && echo "deb http://apt.kubernetes.io/ kubernetes-xenial main" | sudo tee -a /etc/apt/sources.list.d/kubernetes.list \
        && sudo apt-get update \
        && sudo apt-get install kubectl; \
    else sudo apt-get autoremove -y wget; \
    fi;) \
    && sudo rm -rf /var/lib/apt/lists/* \
    && sudo apt-get clean

WORKDIR $HOME
