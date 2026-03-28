ARG BASE_IMAGE
FROM $BASE_IMAGE

COPY python/ray/tests/runtime_env_container/ /home/ray/tests/

RUN pip install --no-cache-dir -c /home/ray/requirements_compiled.txt httpx

# Newer podman than Ubuntu universe so `podman pull docker-daemon:` uses a current Docker
# API client. OBS devel:kubic:libcontainers — use *unstable* for jammy: the *stable* pool
# no longer ships a podman .deb (only helpers like conmon/crun); unstable has podman 4.x.
RUN sudo apt-get update -qq \
    && sudo apt-get install -y -qq ca-certificates curl gnupg \
    && sudo mkdir -p /etc/apt/keyrings \
    && curl -fsSL \
        "https://download.opensuse.org/repositories/devel:/kubic:/libcontainers:/unstable/xUbuntu_22.04/Release.key" \
        | sudo gpg --dearmor -o /etc/apt/keyrings/devel_kubic_libcontainers_unstable.gpg \
    && sudo chmod a+r /etc/apt/keyrings/devel_kubic_libcontainers_unstable.gpg \
    && echo "deb [arch=$(dpkg --print-architecture) signed-by=/etc/apt/keyrings/devel_kubic_libcontainers_unstable.gpg] https://download.opensuse.org/repositories/devel:/kubic:/libcontainers:/unstable/xUbuntu_22.04/ /" \
        | sudo tee /etc/apt/sources.list.d/devel:kubic:libcontainers:unstable.list \
    && sudo apt-get update \
    && sudo apt-get install -y podman
