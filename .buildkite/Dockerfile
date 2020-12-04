FROM ubuntu:focal

ENV DEBIAN_FRONTEND=noninteractive
ENV TZ=America/Los_Angeles
RUN apt-get update -qq && apt-get install -y -qq \
        curl python-is-python3 git build-essential \
        sudo unzip apt-utils dialog tzdata
# RUN useradd -ms /bin/bash -d /home/ray ray \
#         && usermod -a -G root ray \
#         && usermod -aG sudo ray \
#         && echo 'ray ALL=NOPASSWD: ALL' >> /etc/sudoers
# USER ray