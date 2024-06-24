ARG BASE_IMAGE
FROM $BASE_IMAGE

COPY python/ray/tests/runtime_env_container/ /home/ray/tests/

# Install podman
RUN sudo sh -c "echo 'deb http://download.opensuse.org/repositories/devel:/kubic:/libcontainers:/stable/xUbuntu_20.04/ /' > /etc/apt/sources.list.d/devel:kubic:libcontainers:stable.list" && \
    sudo apt-key adv --keyserver hkp://keyserver.ubuntu.com:80 --recv-keys 4D64390375060AA4 && \
    sudo apt-get update && \
    sudo apt-get install podman -y