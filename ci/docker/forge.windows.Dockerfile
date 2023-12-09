FROM mcr.microsoft.com/windows/servercore:ltsc2019

ENV chocolateyVersion=1.4.0

SHELL ["powershell", "-Command", "$ErrorActionPreference = 'Stop'; $ProgressPreference = 'SilentlyContinue';"]
COPY ci/ray_ci/windows/docker/install C:/Install/
WORKDIR C:/Install
# RUN ./main.ps1
# RUN ./java.ps1
RUN ./git.ps1
RUN ./miniconda.ps1
RUN ./bazel.ps1
RUN ./msys.ps1
RUN ./docker.ps1
