ARG BASE_IMAGE
FROM $BASE_IMAGE

COPY python/ray/tests/runtime_env_container/ /home/ray/tests/

# Install podman
RUN pip install --no-cache-dir -c /home/ray/requirements_compiled.txt httpx
RUN sudo apt-get update && sudo apt-get install podman -y
