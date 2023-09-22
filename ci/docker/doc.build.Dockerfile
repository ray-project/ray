ARG DOCKER_IMAGE_BASE_BUILD=cr.ray.io/rayproject/oss-ci-base_build
FROM $DOCKER_IMAGE_BASE_BUILD

# Unset dind settings; we are using the host's docker daemon.
ENV DOCKER_TLS_CERTDIR=
ENV DOCKER_HOST=
ENV DOCKER_TLS_VERIFY=
ENV DOCKER_CERT_PATH=

SHELL ["/bin/bash", "-ice"]

COPY . .

RUN pip install -U --ignore-installed  \
  -c python/requirements_compiled.txt \
  -r python/requirements.txt \
  -r python/requirements/test-requirements.txt \
  -r python/requirements/lint-requirements.txt \
  -r doc/requirements-doc.txt 

RUN HOROVOD_WITH_GLOO=1 HOROVOD_WITHOUT_MPI=1 HOROVOD_WITHOUT_MXNET=1 \
  pip install -U --ignore-installed  -c python/requirements_compiled.txt horovod
