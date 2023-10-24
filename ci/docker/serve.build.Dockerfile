ARG DOCKER_IMAGE_BASE_BUILD=cr.ray.io/rayproject/oss-ci-base_build
FROM $DOCKER_IMAGE_BASE_BUILD

ARG PYTHON_VERSION
ARG PYDANTIC_VERSION

# Install custom Python version if requested.
RUN if [[ -z $PYTHON_VERSION ]] ; then echo Not installing custom Python version ; else PYTHON=$PYTHON_VERSION ci/env/install-dependencies.sh ; fi

# Unset dind settings; we are using the host's docker daemon.
ENV DOCKER_TLS_CERTDIR=
ENV DOCKER_HOST=
ENV DOCKER_TLS_VERIFY=
ENV DOCKER_CERT_PATH=

SHELL ["/bin/bash", "-ice"]

COPY . .

RUN pip install -U torch==1.9.0 torchvision==0.10.0
RUN pip install -U -c python/requirements_compiled.txt \
	tensorflow tensorflow-probability
RUN pip install -U --ignore-installed \
  -c python/requirements_compiled.txt \
  -r python/requirements.txt \
  -r python/requirements/test-requirements.txt

RUN git clone https://github.com/wg/wrk.git /tmp/wrk && pushd /tmp/wrk && make -j && sudo cp wrk /usr/local/bin && popd

# Install custom Pydantic version if requested.
RUN if [[ -z $PYDANTIC_VERSION ]] ; then echo Not installing custom Pydantic version ; else pip install -U pydantic==$PYDANTIC_VERSION ; fi
