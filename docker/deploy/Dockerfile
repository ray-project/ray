# The deploy Docker image build a self-contained Ray instance suitable
# for end users.

FROM ubuntu:xenial
RUN apt-get update
RUN apt-get -y install apt-utils
RUN apt-get -y install sudo
RUN apt-get install -y git cmake build-essential autoconf curl libtool python-dev python-numpy python-pip libboost-all-dev unzip graphviz
RUN pip install ipython funcsigs subprocess32 protobuf colorama graphviz
RUN pip install --upgrade git+git://github.com/cloudpipe/cloudpickle.git@0d225a4695f1f65ae1cbb2e0bbc145e10167cce4  # We use the latest version of cloudpickle because it can serialize named tuples.
RUN adduser --gecos --ingroup ray-user --disabled-login --gecos ray-user
RUN adduser ray-user sudo
RUN sed -i "s|%sudo\tALL=(ALL:ALL) ALL|%sudo\tALL=NOPASSWD: ALL|" /etc/sudoers
USER ray-user
WORKDIR /home/ray-user
RUN git clone https://github.com/ray-project/ray
WORKDIR /home/ray-user/ray
RUN ./setup.sh
RUN ./build.sh
RUN echo '\n# Ray environment\nsource /home/ray-user/ray/setup-env.sh' >> /home/ray-user/.bashrc
ENTRYPOINT bash
