ARG DOCKER_IMAGE_BASE_BUILD=cr.ray.io/rayproject/oss-ci-base_ml
FROM $DOCKER_IMAGE_BASE_BUILD

ARG RAYCI_IS_GPU_BUILD

SHELL ["/bin/bash", "-ice"]

COPY . .

RUN RLLIB_TESTING=1 ./ci/env/install-dependencies.sh

RUN if [[ "$RAYCI_IS_GPU_BUILD" == "true" ]]; then \
  pip install -Ur ./python/requirements/ml/dl-gpu-requirements.txt; \
fi
