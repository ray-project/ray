ARG BASE_IMAGE
FROM rayproject/ray:nightly"$BASE_IMAGE"
ARG PYTHON_MINOR_VERSION=7

# We have to uninstall wrapt this way for Tensorflow compatibility
COPY requirements.txt ./
COPY dl-cpu-requirements.txt ./
COPY dl-gpu-requirements.txt ./
COPY ray-docker-requirements.txt ./
COPY ml-docker-requirements.txt ./
COPY core-requirements.txt ./
COPY data-requirements.txt ./
COPY rllib-requirements.txt ./
COPY rllib-test-requirements.txt ./
COPY tune-requirements.txt ./
COPY tune-test-requirements.txt ./
COPY train-requirements.txt ./
COPY train-test-requirements.txt ./

RUN sudo apt-get update \
    && sudo apt-get install -y gcc \
        cmake \
        libgtk2.0-dev \
        zlib1g-dev \
        libgl1-mesa-dev \
        unzip \
        unrar

RUN $HOME/anaconda3/bin/pip --no-cache-dir install -U pip pip-tools

# Install requirements
RUN $HOME/anaconda3/bin/pip --no-cache-dir install -U \
           -r requirements.txt
 \
    # Install other requirements. Keep pinned requirements bounds as constraints
RUN $HOME/anaconda3/bin/pip --no-cache-dir install -U \
           -c requirements.txt \
           -r core-requirements.txt \
           -r data-requirements.txt \
           -r rllib-requirements.txt \
           -r rllib-test-requirements.txt \
           -r train-requirements.txt \
           -r train-test-requirements.txt \
           -r tune-requirements.txt \
           -r tune-test-requirements.txt \
           -r ray-docker-requirements.txt \
           -r ml-docker-requirements.txt

# explicitly install (overwrite) pytorch with CUDA support
RUN $HOME/anaconda3/bin/pip --no-cache-dir install -U \
           -c requirements.txt \
           -r dl-gpu-requirements.txt \

# Remove dataclasses & typing because they are included in Python > 3.6
RUN if [ $(python -c 'import sys; print(sys.version_info.minor)') != "6" ]; then \
        $HOME/anaconda3/bin/pip uninstall dataclasses typing -y; fi  \
    && sudo rm \
        *requirements.txt \
    && sudo apt-get clean

# Export installed packages
RUN $HOME/anaconda3/bin/pip freeze > /home/ray/pip-freeze.txt

# Make sure tfp is installed correctly and matches tf version.
RUN python -c "import tensorflow_probability"
