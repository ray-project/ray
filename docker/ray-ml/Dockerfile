ARG BASE_IMAGE
FROM rayproject/ray:nightly"$BASE_IMAGE"
ARG PYTHON_MINOR_VERSION=7

# We have to uninstall wrapt this way for Tensorflow compatibility
COPY requirements.txt ./
COPY requirements_no_deps.txt ./
COPY dl-cpu-requirements.txt ./
COPY ml-docker-requirements.txt ./
COPY rllib-requirements.txt ./
COPY tune-requirements.txt ./
COPY train-requirements.txt ./
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
           -r rllib-requirements.txt \
           -r train-requirements.txt \
           -r tune-requirements.txt \
           -r requirements_upstream.txt \
    # explicitly install (overwrite) pytorch with CUDA support
    && $HOME/anaconda3/bin/pip --no-cache-dir install -U \
           -c requirements.txt \
           -r ml-docker-requirements.txt \
    # Remove dataclasses & typing because they are included in Python > 3.6
    && if [ $(python -c 'import sys; print(sys.version_info.minor)') != "6" ]; then \
        $HOME/anaconda3/bin/pip uninstall dataclasses typing -y; fi  \
    && sudo rm \
        requirements*.txt \
    && sudo apt-get clean

RUN $HOME/anaconda3/bin/pip freeze > /home/ray/pip-freeze.txt

# Make sure tfp is installed correctly and matches tf version.
RUN python -c "import tensorflow_probability"
