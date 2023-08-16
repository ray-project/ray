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
ARG PYTHON_VERSION=3.8.16
ARG HOSTTYPE=${HOSTTYPE:-x86_64}

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
# Todo (krfricke): Move to latest miniconda version once we stop building
# images for Python 3.7.
# https://repo.anaconda.com/miniconda/Miniconda3-latest-Linux-${HOSTTYPE}.sh

SHELL ["/bin/bash", "-c"]
RUN sudo apt-get update -y && sudo apt-get upgrade -y \
    && sudo apt-get install -y \
        git \
        libjemalloc-dev \
        wget \
        cmake \
        g++ \ 
        zlib1g-dev \
        $(if [ "$AUTOSCALER" = "autoscaler" ]; then echo \
        tmux \
        screen \
        rsync \
        netbase \
        openssh-client \
        gnupg; fi) \
    && if [[ "${PYTHON_VERSION}" = "3.11" || "${PYTHON_VERSION}" =~ ^3\.11\. ]]; then \
            MINICONDA_VERSION="py310_23.3.1-0"; \
            LIBGCC=(libgcc-ng); \
        else \
            MINICONDA_VERSION="py37_23.1.0-1"; \
            # TODO (can): Remove libffi=3.3 once #33299 is resolved
            LIBGCC=(libgcc-ng libffi=3.3); \
        fi \
    && wget --quiet \
        "https://repo.anaconda.com/miniconda/Miniconda3-${MINICONDA_VERSION}-Linux-${HOSTTYPE}.sh" \
        -O /tmp/miniconda.sh \
    && /bin/bash /tmp/miniconda.sh -b -u -p $HOME/anaconda3 \
    && $HOME/anaconda3/bin/conda init \ 
    && echo 'export PATH=$HOME/anaconda3/bin:$PATH' >> /home/ray/.bashrc \
    && rm /tmp/miniconda.sh  \
    && $HOME/anaconda3/bin/conda install -y "${LIBGCC[@]}" python=$PYTHON_VERSION \
    && $HOME/anaconda3/bin/conda clean -y --all \
    && $HOME/anaconda3/bin/pip install --no-cache-dir \
        flatbuffers \
        cython==0.29.32 \
        # Necessary for Dataset to work properly.
        numpy\>=1.20 \
        psutil \
    # To avoid the following error on Jenkins:
    # AttributeError: 'numpy.ufunc' object has no attribute '__module__'
    && $HOME/anaconda3/bin/pip uninstall -y dask \ 
    # We install cmake temporarily to get psutil
    && sudo apt-get autoremove -y cmake zlib1g-dev \
        # We keep g++ on GPU images, because uninstalling removes CUDA Devel tooling
        $(if [[ "$BASE_IMAGE" == "ubuntu:focal" && "$HOSTTYPE" == "x86_64" ]]; then echo \
        g++; fi) \
    # Either install kubectl or remove wget 
    && (if [ "$AUTOSCALER" = "autoscaler" ]; \
        then sudo mkdir -m 755 /etc/apt/keyrings \
        && wget -O - -q https://pkgs.k8s.io/core:/stable:/v1.28/deb/Release.key | sudo gpg --dearmor -o /etc/apt/keyrings/kubernetes-apt-keyring.gpg \
        && sudo touch /etc/apt/sources.list.d/kubernetes.list \
        && echo "deb [signed-by=/etc/apt/keyrings/kubernetes-apt-keyring.gpg] https://pkgs.k8s.io/core:/stable:/v1.28/deb/ /" | sudo tee -a /etc/apt/sources.list.d/kubernetes.list \
        && sudo apt-get update \
        && sudo apt-get install kubectl; \
    else sudo apt-get autoremove -y wget; \
    fi;) \
    && sudo rm -rf /var/lib/apt/lists/* \
    && sudo apt-get clean

WORKDIR $HOME
