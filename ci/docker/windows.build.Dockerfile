ARG DOCKER_IMAGE_BASE_BUILD=rayproject/buildenv:windows
FROM $DOCKER_IMAGE_BASE_BUILD

ENV PYTHON=3.10
ENV PYTHON_FULL_VERSION=3.10.19
ENV RAY_BUILD_ENV=win_py$PYTHON_FULL_VERSION

# build_base.sh builds the test interpreter in a dedicated conda env rather than
# swapping the base env's python in place. Put that env ahead of the base
# Miniconda install on PATH so `python`/`pip` -- used by build_ray.sh and by
# bazel at test time -- resolve to it. This mirrors the PATH order that
# `conda activate ray` would set, but bakes it into the image so it survives
# into the non-login, non-interactive test shells.
ENV RAY_CONDA_ENV=ray
ENV CONDA_PREFIX="C:\\Miniconda3\\envs\\ray"
ENV CONDA_DEFAULT_ENV=ray
ENV PATH="C:\\Miniconda3\\envs\\ray;C:\\Miniconda3\\envs\\ray\\Library\\mingw-w64\\bin;C:\\Miniconda3\\envs\\ray\\Library\\usr\\bin;C:\\Miniconda3\\envs\\ray\\Library\\bin;C:\\Miniconda3\\envs\\ray\\Scripts;C:\\Miniconda3\\envs\\ray\\bin;${PATH}"

COPY . .
RUN bash ci/ray_ci/windows/build_base.sh
