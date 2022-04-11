ARG BASE_IMAGE
FROM rayproject/ray:nightly"$BASE_IMAGE"
ARG PYTHON_MINOR_VERSION=7

# We have to uninstall wrapt this way for Tensorflow compatibility
COPY requirements.txt ./
COPY requirements_dl.txt ./
COPY requirements_ml_docker.txt ./
COPY requirements_rllib.txt ./
COPY requirements_tune.txt ./requirements_tune.txt
COPY requirements_train.txt ./
COPY requirements_upstream.txt ./

RUN sudo apt-get update \
    && sudo apt-get install -y gcc \
        cmake \
        libgtk2.0-dev \
        zlib1g-dev \
        libgl1-mesa-dev \
        unzip \
        unrar \
    && $HOME/anaconda3/bin/pip --no-cache-dir install -U pip \
    && $HOME/anaconda3/bin/pip --no-cache-dir install -U \
           -r requirements.txt \
           -r requirements_rllib.txt \
           -r requirements_train.txt \
           -r requirements_tune.txt \
           -r requirements_upstream.txt \
    # explicitly install (overwrite) pytorch with CUDA support
    && $HOME/anaconda3/bin/pip --no-cache-dir install -U \
           -r requirements_ml_docker.txt \
    # Remove dataclasses & typing because they are included in Python > 3.6
    && if [ $(python -c 'import sys; print(sys.version_info.minor)') != "6" ]; then \
        $HOME/anaconda3/bin/pip uninstall dataclasses typing -y; fi  \
    && sudo rm \
        requirements.txt \
        requirements_ml_docker.txt \
        requirements_tune.txt \
        requirements_rllib.txt \
        requirements_train.txt \
        requirements_upstream.txt \
    && sudo apt-get clean

# Make sure tfp is installed correctly and matches tf version.
RUN python -c "import tensorflow_probability"
