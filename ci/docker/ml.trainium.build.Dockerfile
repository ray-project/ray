ARG DOCKER_IMAGE_BASE_BUILD=cr.ray.io/rayproject/oss-ci-base_ml
FROM $DOCKER_IMAGE_BASE_BUILD

ARG IS_GPU_BUILD

# Unset dind settings; we are using the host's docker daemon.
ENV DOCKER_TLS_CERTDIR=
ENV DOCKER_HOST=
ENV DOCKER_TLS_VERIFY=
ENV DOCKER_CERT_PATH=

SHELL ["/bin/bash", "-ice"]

COPY . .

# TODO (can): Move mosaicml to train-test-requirements.txt
RUN pip install "mosaicml==0.12.1"
RUN TRAIN_TESTING=1 TUNE_TESTING=1 DATA_PROCESSING_TESTING=1 INSTALL_HOROVOD=1 \
  ./ci/env/install-dependencies.sh

RUN if [[ "$IS_GPU_BUILD" == "true" ]]; then \
  pip install -Ur ./python/requirements/ml/dl-gpu-requirements.txt; \
  fi

# Install AWS Trainium Drivers
RUN sudo tee /etc/apt/sources.list.d/neuron.list > /dev/null <<EOF
RUN deb https://apt.repos.neuron.amazonaws.com ${VERSION_CODENAME} main
RUN EOF
RUN wget -qO - https://apt.repos.neuron.amazonaws.com/GPG-PUB-KEY-AMAZON-AWS-NEURON.PUB | sudo apt-key add -
RUN sudo apt-get update -y
RUN sudo apt-get install aws-neuronx-collectives=2.* -y
RUN sudo apt-get install aws-neuronx-runtime-lib=2.* -y
RUN sudo apt-get install aws-neuronx-tools=2.* -y
RUN pip config set global.extra-index-url https://pip.repos.neuron.amazonaws.com
RUN pip install neuronx-cc==2.* torch-neuronx torchvision
