FROM mcr.microsoft.com/windows/servercore:ltsc2019

ENV chocolateyVersion=1.4.0

SHELL ["powershell", "-Command", "$ErrorActionPreference = 'Stop'; $ProgressPreference = 'SilentlyContinue';"]
COPY ci/ray_ci/windows/docker/install C:/Install/
WORKDIR C:/Install
RUN ./main.ps1
