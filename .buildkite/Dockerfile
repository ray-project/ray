FROM ubuntu:focal
RUN apt-get update && apt-get install -y curl python-is-python3 git build-essential
RUN useradd -ms /bin/bash -d /home/ray ray root
USER ray