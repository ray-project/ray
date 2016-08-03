# The devel Docker image is designed for use with a source checkout
# mounted from the local host filesystem.

FROM ubuntu:xenial
RUN apt-get update
RUN apt-get -y install apt-utils
RUN apt-get -y install sudo
RUN apt-get install -y git cmake build-essential autoconf curl libtool python-dev python-numpy python-pip libboost-all-dev unzip graphviz
RUN pip install ipython typing funcsigs subprocess32 protobuf==3.0.0a2 colorama graphviz cloudpickle
RUN adduser --gecos --ingroup ray-user --disabled-login --gecos ray-user --uid 500
RUN adduser ray-user sudo
RUN sed -i "s|%sudo\tALL=(ALL:ALL) ALL|%sudo\tALL=NOPASSWD: ALL|" /etc/sudoers
USER ray-user
WORKDIR /home/ray-user
RUN mkdir /home/ray-user/ray-build
RUN mkdir /home/ray-user/ray
RUN ln -s /home/ray-user/ray-build /home/ray-user/ray/build
ENTRYPOINT bash
