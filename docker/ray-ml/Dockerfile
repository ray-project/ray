ARG BASE_IMAGE
FROM rayproject/ray:nightly"$BASE_IMAGE"

# We have to uninstall wrapt this way for Tensorflow compatibility
COPY requirements.txt ./
COPY dl-cpu-requirements.txt ./
COPY dl-gpu-requirements.txt ./
COPY ray-docker-requirements.txt ./
COPY core-requirements.txt ./
COPY data-requirements.txt ./
COPY rllib-requirements.txt ./
COPY rllib-test-requirements.txt ./
COPY tune-requirements.txt ./
COPY tune-test-requirements.txt ./
COPY train-requirements.txt ./
COPY train-test-requirements.txt ./

COPY install-ml-docker-requirements.sh ./

RUN sudo chmod +x install-ml-docker-requirements.sh && ./install-ml-docker-requirements.sh

# Export installed packages
RUN $HOME/anaconda3/bin/pip freeze > /home/ray/pip-freeze.txt

# Make sure tfp is installed correctly and matches tf version.
RUN python -c "import tensorflow_probability"
