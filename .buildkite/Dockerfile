FROM ubuntu:focal
RUN apt-get update && apt-get install -y \
        curl python-is-python3 git build-essential sudo
# RUN useradd -ms /bin/bash -d /home/ray ray \
#         && usermod -a -G root ray \
#         && usermod -aG sudo ray \
#         && echo 'ray ALL=NOPASSWD: ALL' >> /etc/sudoers
# USER ray