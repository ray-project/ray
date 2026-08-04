ARG BASE_IMAGE
FROM $BASE_IMAGE

COPY python/ray/tests/runtime_env_container/ /home/ray/tests/

RUN pip install --no-cache-dir -c /home/ray/requirements_compiled.txt httpx

# Podman for runtime tests; docker-ce-cli so `docker save | podman load` can talk to
# the mounted host socket (stock podman's docker-daemon: pull uses an old API).
RUN sudo apt-get update -qq \
    && sudo apt-get install -y -qq ca-certificates curl gnupg \
    && sudo mkdir -p /etc/apt/keyrings \
    && curl -fsSL https://download.docker.com/linux/ubuntu/gpg | sudo gpg --dearmor -o /etc/apt/keyrings/docker.gpg \
    && sudo chmod a+r /etc/apt/keyrings/docker.gpg \
    && echo "deb [arch=$(dpkg --print-architecture) signed-by=/etc/apt/keyrings/docker.gpg] https://download.docker.com/linux/ubuntu jammy stable" | sudo tee /etc/apt/sources.list.d/docker.list > /dev/null \
    && sudo apt-get update \
    && sudo apt-get install -y docker-ce-cli podman
