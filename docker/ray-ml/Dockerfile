ARG GPU
FROM rayproject/ray:nightly"$GPU"
ARG PYTHON_MINOR_VERSION=7

# We have to uninstall wrapt this way for Tensorflow compatibility
COPY requirements.txt ./
COPY requirements_ml_docker.txt ./
COPY requirements_rllib.txt ./
COPY requirements_tune.txt ./requirements_tune.txt
COPY install_atari_roms.sh ./install_atari_roms.sh

RUN sudo apt-get update \
    && sudo apt-get install -y gcc \
        cmake \
        libgtk2.0-dev \
        zlib1g-dev \
        libgl1-mesa-dev \
        unzip \
        unrar \
    && $HOME/anaconda3/bin/pip install -U pip \
    && $HOME/anaconda3/bin/pip --use-deprecated=legacy-resolver --no-cache-dir install -r requirements.txt \
    && $HOME/anaconda3/bin/pip --no-cache-dir install -r requirements_rllib.txt \
    && $HOME/anaconda3/bin/pip --no-cache-dir install -r requirements_tune.txt \
    && $HOME/anaconda3/bin/pip --no-cache-dir install -U -r requirements_ml_docker.txt \
    # Remove dataclasses & typing because they are included in Python > 3.6
    && if [ $(python -c 'import sys; print(sys.version_info.minor)') != "6" ]; then \
        $HOME/anaconda3/bin/pip uninstall dataclasses typing -y; fi  \
    && sudo rm requirements.txt && sudo rm requirements_ml_docker.txt \
    && sudo rm requirements_tune.txt && sudo rm requirements_rllib.txt \
    && sudo apt-get clean

# Install Atari ROMs. Previously these have been shipped with atari_py \
RUN ./install_atari_roms.sh
