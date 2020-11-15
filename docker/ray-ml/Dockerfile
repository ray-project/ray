ARG GPU
FROM rayproject/ray:nightly"$GPU"

# We have to uninstall wrapt this way for Tensorflow compatibility
COPY requirements.txt ./
COPY requirements_ml_docker.txt ./
COPY requirements_rllib.txt ./
COPY requirements_tune.txt ./

RUN sudo apt-get update \
    && sudo apt-get install -y gcc \
        cmake \
        libgtk2.0-dev \
        zlib1g-dev \
        libgl1-mesa-dev \
    && $HOME/anaconda3/bin/pip --no-cache-dir install -r requirements.txt \ 
    && $HOME/anaconda3/bin/pip --no-cache-dir install -r requirements_ml_docker.txt \
    && sudo rm requirements.txt && sudo rm requirements_ml_docker.txt \
    && sudo apt-get remove cmake gcc -y \
    && sudo apt-get clean

