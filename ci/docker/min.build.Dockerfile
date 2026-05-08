# syntax=docker/dockerfile:1.3-labs

ARG PYTHON_VERSION=3.10
ARG DOCKER_IMAGE_BASE_BUILD=cr.ray.io/rayproject/oss-ci-base_test-py$PYTHON_VERSION
FROM $DOCKER_IMAGE_BASE_BUILD

ARG PYTHON_VERSION
ARG EXTRA_DEPENDENCY

SHELL ["/bin/bash", "-ice"]

COPY . .

RUN <<EOF
#!/bin/bash

set -euo pipefail

# minimal dependencies
BUILD=1 MINIMAL_INSTALL=1 PYTHON=${PYTHON_VERSION} ./ci/ci.sh init
rm -rf python/ray/thirdparty_files

# Re-activate conda environment (ci.sh runs in a subprocess so PATH changes are lost)
# For Python 3.14+, a separate conda env is created and symlinked to /opt/miniforge/bin
# We can't use `conda activate` because the conda command itself breaks after symlinking
# (it tries to use the new Python which doesn't have conda installed)
if [[ -d /opt/miniforge/envs/py${PYTHON_VERSION} ]]; then
  export PATH="/opt/miniforge/envs/py${PYTHON_VERSION}/bin:/opt/miniforge/bin:$PATH"
else
  export PATH="/opt/miniforge/bin:$PATH"
fi

# install test requirements
python -m pip install -U pytest==8.3.3 pip-tools==7.4.1

# Verify pytest was installed and is accessible
python -m pytest --version || echo "WARNING: pytest not found in PATH"

# install extra dependencies
if [[ "${EXTRA_DEPENDENCY}" == "core" ]]; then
  pip-compile -o min_requirements.txt python/setup.py
elif [[ "${EXTRA_DEPENDENCY}" == "ml" ]]; then
  pip-compile -o min_requirements.txt python/setup.py --extra tune
elif [[ "${EXTRA_DEPENDENCY}" == "default" ]]; then
  pip-compile -o min_requirements.txt python/setup.py --extra default
elif [[ "${EXTRA_DEPENDENCY}" == "serve" ]]; then
  echo "httpx==0.27.2" >> /tmp/min_build_requirements.txt
  echo "pytest-asyncio==1.1.0" >> /tmp/min_build_requirements.txt
  pip-compile -o min_requirements.txt /tmp/min_build_requirements.txt python/setup.py --extra "serve-grpc"
  rm /tmp/min_build_requirements.txt
fi

pip install -r min_requirements.txt

# Core wants to eagerly be tested with some of its prerelease dependencies.
if [[ "${EXTRA_DEPENDENCY}" == "core" ]]; then
  ./ci/env/install-core-prerelease-dependencies.sh
fi

# Verify pytest was installed
python -m pytest --version

EOF

# For Python 3.14+, the PATH needs to include the conda environment directory
# Create a symlink from the env-specific python to a standard location
RUN bash -c 'if [[ -d /opt/miniforge/envs/py${PYTHON_VERSION}/bin ]]; then \
      for f in /opt/miniforge/envs/py${PYTHON_VERSION}/bin/*; do \
        ln -sf "$f" "/opt/miniforge/bin/$(basename "$f")" 2>/dev/null || true; \
      done \
    fi'

# Ensure /opt/miniforge/bin is in PATH for all shells (including non-interactive)
# This is needed for tests.env.Dockerfile which uses bash -i
# For Python 3.14+, also add the env-specific bin directory to PATH
# so that console scripts installed later (like 'ray') are accessible
ENV PATH="/opt/miniforge/bin:/opt/miniforge/envs/py${PYTHON_VERSION}/bin:${PATH}"
