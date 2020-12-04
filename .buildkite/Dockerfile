FROM ubuntu:focal

ENV DEBIAN_FRONTEND=noninteractive
ENV TZ=America/Los_Angeles

RUN apt-get update -qq
RUN apt-get install -y -qq \
    curl python-is-python3 git build-essential \
    sudo unzip apt-utils dialog tzdata wget

RUN cd /tmp \
    && wget -q https://download.docker.com/linux/static/stable/x86_64/docker-19.03.9.tgz \
    && tar zxvf docker-19.03.9.tgz \
    && cp docker/* /bin/ \
    && cd / \
    && docker --version

COPY bazel-credential.json /bazel-credential.json
RUN mkdir /bazel-cache; \
    (echo "build --disk_cache=/bazel-cache" >> /root/.bazelrc); \
    (echo "build --repository_cache=/bazel-cache" >> /root/.bazelrc); \
    (echo "build --remote_cache=https://storage.googleapis.com/ray-bazel-cache" >> /root/.bazelrc); \
    (echo "build --google_credentials=/bazel-credential.json" >> /root/.bazelrc) && \
    cat /root/.bazelrc