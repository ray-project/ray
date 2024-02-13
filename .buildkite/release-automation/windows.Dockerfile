ARG DOCKER_IMAGE_BASE_BUILD=rayproject/buildenv:windows
FROM $DOCKER_IMAGE_BASE_BUILD

COPY . .
RUN .buildkite/release-automation/build_windows.sh
SHELL ["powershell", "-Command", "$ErrorActionPreference = 'Stop'; $ProgressPreference = 'SilentlyContinue';"]

RUN Set-ExecutionPolicy Bypass -Scope Process -Force; [System.Net.ServicePointManager]::SecurityProtocol = [System.Net.ServicePointManager]::SecurityProtocol -bor 3072; iex ((New-Object System.Net.WebClient).DownloadString('https://community.chocolatey.org/install.ps1'))
RUN choco install git.install --params "'/WindowsTerminal /NoAutoCrlf'"
#RUN choco install anaconda3 -y --force --params '"/JustMe /AddToPath"'

SHELL ["cmd", "/S", "/C"]