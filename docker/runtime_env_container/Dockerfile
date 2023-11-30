ARG BASE_IMAGE
ARG FULL_BASE_IMAGE=rayproject/ray-deps:nightly"$BASE_IMAGE"
FROM $FULL_BASE_IMAGE

ARG WHEEL_PATH
ARG FIND_LINKS_PATH=".whl"
ARG CONSTRAINTS_FILE="requirements_compiled.txt"

COPY requirements_compiled.txt ./
COPY $WHEEL_PATH .
COPY $FIND_LINKS_PATH $FIND_LINKS_PATH

RUN $HOME/anaconda3/bin/pip --no-cache-dir install -c $CONSTRAINTS_FILE \
    `basename $WHEEL_PATH`[all] \
    --find-links $FIND_LINKS_PATH && sudo rm `basename $WHEEL_PATH`

RUN $HOME/anaconda3/bin/pip freeze > /home/ray/pip-freeze.txt

# Install podman
RUN sudo apt install software-properties-common uidmap -y && \
    sudo sh -c "echo 'deb http://download.opensuse.org/repositories/devel:/kubic:/libcontainers:/stable/xUbuntu_20.04/ /' > /etc/apt/sources.list.d/devel:kubic:libcontainers:stable.list" && \
    sudo apt-key adv --keyserver hkp://keyserver.ubuntu.com:80 --recv-keys 4D64390375060AA4 && \
    sudo apt-get update && \
    sudo apt-get install podman -y