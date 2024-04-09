ARG BASE_IMAGE="ubuntu:22.04"
FROM ${BASE_IMAGE}
# FROM directive resets ARG
ARG BASE_IMAGE
# If this arg is not "autoscaler" then no autoscaler requirements will be included
ARG AUTOSCALER="autoscaler"
ENV TZ=America/Los_Angeles

ARG HOSTTYPE=${HOSTTYPE:-x86_64}

ENV PATH "/root/anaconda3/bin:$PATH"
ARG DEBIAN_FRONTEND=noninteractive
ARG PYTHON_VERSION=3.7.16

RUN apt-get update -y \
    && apt-get install -y sudo tzdata \
    && rm -rf /var/lib/apt/lists/* \
    && apt-get clean

RUN apt-get update -y && sudo apt-get upgrade -y \
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
        unzip \
    && sudo apt install python3 pip -y \
    && sudo ln -s /usr/bin/python3 /usr/bin/python \
    && wget -O /tmp/Miniforge3.sh "https://github.com/conda-forge/miniforge/releases/latest/download/Miniforge3-$(uname)-$(uname -m).sh" \
    && bash /tmp/Miniforge3.sh -b -p "${HOME}/conda" \
    && source "${HOME}/conda/etc/profile.d/conda.sh" \
    && echo "export PATH=$HOME/.local/bin:$PATH" >> /home/ray/.bashrc \
    && rm /tmp/Miniforge3.sh  \
    && $HOME/conda/condabin/conda install -y libgcc-ng python=$PYTHON_VERSION \
    && $HOME/conda/condabin/conda install -y -c conda-forge libffi=3.4.2 \
    && $HOME/conda/condabin/conda clean -y --all \
    && pip install --no-cache-dir \
        flatbuffers \
        cython==0.29.32 \
        numpy==1.15.4 \
        psutil \
    # To avoid the following error on Jenkins:
    # AttributeError: 'numpy.ufunc' object has no attribute '__module__'
    && pip uninstall -y dask \
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

RUN apt-get update -y \
    && apt-get install -y gnupg curl golang wget make git libseccomp-dev \
    #&& . /etc/os-release \
    #&& echo "deb https://download.opensuse.org/repositories/devel:/kubic:/libcontainers:/stable/xUbuntu_${VERSION_ID}/ /" | tee /etc/apt/sources.list.d/devel:kubic:libcontainers:stable.list \
    #&& curl -L https://download.opensuse.org/repositories/devel:/kubic:/libcontainers:/stable/xUbuntu_${VERSION_ID}/Release.key | apt-key add - \
    #&& apt-get update \
    && apt-get -y install podman runc \
    && rm -rf /var/lib/apt/lists/* \
    && apt-get clean

ARG WHEEL_PATH
ARG FIND_LINKS_PATH=".whl"
# For Click
ENV LC_ALL=C.UTF-8
ENV LANG=C.UTF-8
COPY $WHEEL_PATH .
COPY $FIND_LINKS_PATH $FIND_LINKS_PATH
RUN $HOME/anaconda3/bin/pip --no-cache-dir install `basename $WHEEL_PATH`[all] \
--find-links $FIND_LINKS_PATH && sudo rm `basename $WHEEL_PATH`
