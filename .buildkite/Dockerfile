FROM ubuntu:focal
RUN apt-get update && apt-get install -y curl
RUN useradd -ms /bin/bash -d /home/ray ray --uid 1000 --gid 100
USER ubuntu