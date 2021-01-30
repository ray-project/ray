# The base-deps Docker image installs main libraries needed to run Ray

# The GPU option is nvidia/cuda:11.2.0-cudnn8-devel-ubuntu18.04
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
ARG PYTHON_VERSION=3.8.6

ARG RAY_UID=1000
ARG RAY_GID=100

RUN apt-get update -y \
    && apt-get install -y sudo tzdata bash \
    && useradd -ms /bin/bash -d /home/ray ray --uid $RAY_UID --gid $RAY_GID \
    && usermod -aG sudo ray \
    && echo 'ray ALL=NOPASSWD: ALL' >> /etc/sudoers \
    && rm -rf /var/lib/apt/lists/* \
    && apt-get clean

USER $RAY_UID
ENV HOME=/home/ray

SHELL ["/bin/bash", "-c"]


RUN  export arch=$(uname -m); \
     if [ "$arch" == "aarm64" ]; then \
	arch="arm64"; \
     fi; \
    sudo apt-get update -y && sudo apt-get upgrade -y \
    && sudo apt-get install -y \
	git \
	wget \
	cmake \
	gcc \
	g++ \
	make \
	zlib1g-dev \
	zlib1g \
	$(if [ "$AUTOSCALER" == "autoscaler" ]; then echo \
	tmux \
	screen \
	rsync \
	openssh-client \
	gnupg; fi) \
     && wget \
	--quiet "https://github.com/conda-forge/miniforge/releases/download/4.8.5-1/Miniforge3-4.8.5-1-Linux-${arch}.sh" \
	-O /tmp/miniconda.sh \
    && /bin/bash /tmp/miniconda.sh -b -u -p $HOME/anaconda3 \
    && $HOME/anaconda3/bin/conda init \
    && echo 'export PATH=$HOME/anaconda3/bin:$PATH' >> /home/ray/.bashrc \
    && rm /tmp/miniconda.sh \
    && $HOME/anaconda3/bin/conda install -y mamba \
    && $HOME/anaconda3/bin/mamba install --yes python=$PYTHON_VERSION \
    && $HOME/anaconda3/bin/mamba install --yes cytoolz lz4 pandas numpy==1.19.2 scikit-build flatbuffers cython==0.29.21 psutil pynacl \
    && $HOME/anaconda3/bin/conda clean -y --all \
    && $HOME/anaconda3/bin/pip install --no-cache-dir \
	flatbuffers \
	cython==0.29.23 \
	numpy==1.19.5 \
	psutil \
	blist \
	atari-py \
    # blist is needed for numpy (which is re-installed when ray is installed)
    # atari-py is built from source for Python 3.8 (requires g++ & zlib1g-dev)
    # To avoid the following error on Jenkins:
    # AttributeError: 'numpy.ufunc' object has no attribute '__module__'
    && $HOME/anaconda3/bin/pip uninstall -y dask \
    # We install cmake temporarily to get psutil, blist & atari-py
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
