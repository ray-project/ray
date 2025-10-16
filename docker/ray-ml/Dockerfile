# syntax=docker/dockerfile:1.3-labs

ARG BASE_IMAGE
ARG FULL_BASE_IMAGE=rayproject/ray:nightly"$BASE_IMAGE"
FROM "$FULL_BASE_IMAGE"

COPY python/*requirements.txt \
     python/requirements/ml/*requirements.txt  \
     python/requirements/docker/*requirements.txt ./
COPY docker/ray-ml/install-ml-docker-requirements.sh ./
COPY docker/ray-ml/install-gdrcopy.sh ./

RUN sudo chmod +x install-ml-docker-requirements.sh \
    && ./install-ml-docker-requirements.sh
RUN sudo apt-get update && sudo apt-get install -y curl
RUN sudo chmod +x install-gdrcopy.sh \
    && sudo GDRCOPY_VERSION=2.4-1 ./install-gdrcopy.sh Ubuntu22_04 12.2 x64

# Export installed packages
RUN $HOME/anaconda3/bin/pip freeze > /home/ray/pip-freeze.txt

# Make sure tfp is installed correctly and matches tf version.
RUN python -c "import tensorflow_probability"
