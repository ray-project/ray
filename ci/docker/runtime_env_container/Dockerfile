ARG BASE_IMAGE
FROM $BASE_IMAGE

COPY python/ray/tests/runtime_env_container/ /home/ray/tests/

# Install podman
RUN sudo apt-get update && sudo apt-get install podman -y
