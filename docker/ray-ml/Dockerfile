ARG GPU
FROM rayproject/ray:nightly"$GPU"

RUN mkdir ./requirements
# We have to uninstall wrapt this way for Tensorflow compatibility
COPY requirements.txt ./
COPY requirements_ml_docker.txt ./
COPY requirements_rllib.txt ./
COPY requirements/requirements_tune.txt ./requirements/

RUN sudo apt-get update \
    && sudo apt-get install -y gcc \
        cmake \
        libgtk2.0-dev \
        zlib1g-dev \
        libgl1-mesa-dev \
    && $HOME/anaconda3/bin/pip --use-deprecated=legacy-resolver --no-cache-dir install -r requirements.txt \ 
    && $HOME/anaconda3/bin/pip --use-deprecated=legacy-resolver --no-cache-dir install -r requirements_ml_docker.txt \
    # Remove dataclasses & typing because they are included in Py3.7
    && $HOME/anaconda3/bin/pip uninstall dataclasses typing -y  \ 
    && sudo rm requirements.txt && sudo rm requirements_ml_docker.txt \
    && sudo rm -rf requirements && sudo rm -rf requirements_rllib.txt
    && sudo apt-get clean

