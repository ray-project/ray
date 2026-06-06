ARG DOCKER_IMAGE_BASE_BUILD=rayproject/buildenv:windows
FROM $DOCKER_IMAGE_BASE_BUILD

ENV PYTHON=3.10
ENV PYTHON_FULL_VERSION=3.10.19
ENV RAY_BUILD_ENV=win_py$PYTHON_FULL_VERSION

# build_base.sh builds the test interpreter in a dedicated conda env ("ray")
# rather than swapping the base env's python in place.
ENV RAY_CONDA_ENV=ray

COPY . .
RUN bash ci/ray_ci/windows/build_base.sh

# Make the dedicated env the default interpreter for build_ray.sh and for bazel
# at test time. On this base image the effective PATH lives in the Windows
# registry (machine scope), NOT in a Docker ENV -- so we prepend the env there.
# Using `ENV PATH=...;${PATH}` would expand ${PATH} to nothing and wipe
# conda/git/bash off the PATH (CreateProcess: file not found). `$env:Path` below
# is expanded by powershell at build time (Docker does not substitute in RUN).
RUN powershell -Command "[Environment]::SetEnvironmentVariable('Path', 'C:\Miniconda3\envs\ray;C:\Miniconda3\envs\ray\Library\mingw-w64\bin;C:\Miniconda3\envs\ray\Library\usr\bin;C:\Miniconda3\envs\ray\Library\bin;C:\Miniconda3\envs\ray\Scripts;C:\Miniconda3\envs\ray\bin;' + $env:Path, 'Machine')"
