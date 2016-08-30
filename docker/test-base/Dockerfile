# Base image for tests. This differs from the deploy image in that
# rather than downloading source code from github it instead adds
# it from a tar file creaetd by test/travis-ci/install.sh

FROM ubuntu:xenial
RUN apt-get update
RUN apt-get -y install apt-utils
RUN apt-get -y install sudo
RUN apt-get install -y git cmake build-essential autoconf curl libtool python-dev python-numpy python-pip libboost-all-dev unzip graphviz
RUN pip install ipython funcsigs subprocess32 protobuf colorama graphviz cloudpickle
RUN adduser --gecos --ingroup ray-user --disabled-login --gecos ray-user
RUN adduser ray-user sudo
RUN sed -i "s|%sudo\tALL=(ALL:ALL) ALL|%sudo\tALL=NOPASSWD: ALL|" /etc/sudoers
ADD ray.tar /home/ray-user/ray
RUN chown -R ray-user.ray-user /home/ray-user/ray
USER ray-user
WORKDIR /home/ray-user/ray
RUN ./setup.sh
RUN ./build.sh
