ARG BASE_IMAGE
FROM rayproject/ray:nightly"$BASE_IMAGE"
ARG PYTHON_MINOR_VERSION=7

# We have to uninstall wrapt this way for Tensorflow compatibility
COPY requirements.txt ./
COPY requirements_no_deps.txt ./
COPY requirements_dl.txt ./
COPY requirements_ml_docker.txt ./
COPY requirements_rllib.txt ./
COPY requirements_tune.txt ./
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
    && $HOME/anaconda3/bin/pip --no-cache-dir install -U pip pip-tools \
    # Install no-deps requirements. Their dependencies may be overwritten
    # in subsequent steps
    && $HOME/anaconda3/bin/pip --no-cache-dir install -U \
           -r requirements_no_deps.txt \
        # Then, install requirements
    && $HOME/anaconda3/bin/pip --no-cache-dir install -U \
           -r requirements.txt \
    # Install other requirements. Keep pinned requirements bounds as constraints
    && $HOME/anaconda3/bin/pip --no-cache-dir install -U \
           -c requirements.txt \
           -r requirements_rllib.txt \
           -r requirements_train.txt \
           -r requirements_tune.txt \
           -r requirements_upstream.txt \
    # explicitly install (overwrite) pytorch with CUDA support
    && $HOME/anaconda3/bin/pip --no-cache-dir install -U \
           -c requirements.txt \
           -r requirements_ml_docker.txt \
    # Remove dataclasses & typing because they are included in Python > 3.6
    && if [ $(python -c 'import sys; print(sys.version_info.minor)') != "6" ]; then \
        $HOME/anaconda3/bin/pip uninstall dataclasses typing -y; fi  \
    && sudo rm \
        requirements*.txt \
    && sudo apt-get clean

RUN $HOME/anaconda3/bin/pip freeze > /home/ray/pip-freeze.txt

# Make sure tfp is installed correctly and matches tf version.
RUN python -c "import tensorflow_probability"
