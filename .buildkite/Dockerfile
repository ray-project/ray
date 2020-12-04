FROM ubuntu:focal
RUN apt-get update && apt-get install -y curl python-is-python3 git build-essential sudo unzip apt-utils dialog tzdata
RUN useradd -ms /bin/bash -d /home/ray ray && usermod -a -G root ray
USER ray

