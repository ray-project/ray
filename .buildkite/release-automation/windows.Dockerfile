FROM "029272617770.dkr.ecr.us-west-2.amazonaws.com/rayproject/citemp:87ad4d5b-windowsbuild"

ARG BUILDKITE_BAZEL_CACHE_URL
ARG BUILDKITE_PIPELINE_ID

ENV BUILDKITE_BAZEL_CACHE_URL=${BUILDKITE_BAZEL_CACHE_URL}
ENV BUILDKITE_PIPELINE_ID=${BUILDKITE_PIPELINE_ID}
ENV PYTHON=3.9
ENV RAY_USE_RANDOM_PORTS=1
ENV RAY_DEFAULT_BUILD=1
ENV RAY_INSTALL_JAVA=0
ENV RAY_ENABLE_WINDOWS_OR_OSX_CLUSTER=1
ENV LC_ALL=en_US.UTF-8
ENV LANG=en_US.UTF-8

COPY . .
RUN bash ci/ray_ci/windows/build_ray.sh
SHELL ["powershell", "-Command", "$ErrorActionPreference = 'Stop'; $ProgressPreference = 'SilentlyContinue';"]

RUN Set-ExecutionPolicy Bypass -Scope Process -Force; [System.Net.ServicePointManager]::SecurityProtocol = [System.Net.ServicePointManager]::SecurityProtocol -bor 3072; iex ((New-Object System.Net.WebClient).DownloadString('https://community.chocolatey.org/install.ps1'))
RUN choco install git.install --params "'/WindowsTerminal /NoAutoCrlf'"
SHELL ["cmd", "/S", "/C"]

WORKDIR /rayci
