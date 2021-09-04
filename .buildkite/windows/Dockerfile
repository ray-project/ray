FROM mcr.microsoft.com/windows/servercore:ltsc2019

SHELL ["powershell", "-Command", "$ErrorActionPreference = 'Stop'; $ProgressPreference = 'SilentlyContinue';"]
COPY install C:/Install/
WORKDIR C:/Install

RUN Write-Output "Dry run windows docker build"
# RUN ./main.ps1
# RUN ./java.ps1
# RUN ./miniconda.ps1
# RUN ./git.ps1
# RUN ./bazel.ps1
# RUN ./msys.ps1
# RUN ./ray.ps1
