# syntax=docker/dockerfile:1.3-labs

ARG BASE_IMAGE
ARG FULL_BASE_IMAGE=rayproject/ray:nightly"$BASE_IMAGE"
FROM "$FULL_BASE_IMAGE"

ARG ARCH
ENV NVARCH=${NVARCH}

RUN if [ "$NVARCH" = "x86_64" ]; then \
        ARCH="x64"; \
    else \
        ARCH="aarch64"; \
    fi && \
    echo "Using ARCH=$ARCH"

COPY python/*requirements.txt \
     python/requirements/ml/*requirements.txt  \
     python/requirements/docker/*requirements.txt ./
COPY docker/ray-ml/install-ml-docker-requirements.sh ./
COPY ci/env/install-gdrcopy.sh ./

RUN sudo chmod +x install-ml-docker-requirements.sh \
    && ./install-ml-docker-requirements.sh
RUN sudo chmod +x install-gdrcopy.sh \
    && ./install-gdrcopy.sh ubuntu22.04 12.2 "$ARCH"

# Export installed packages
RUN $HOME/anaconda3/bin/pip freeze > /home/ray/pip-freeze.txt

# Make sure tfp is installed correctly and matches tf version.
RUN python -c "import tensorflow_probability"
