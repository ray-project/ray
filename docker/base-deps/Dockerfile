# The deploy Docker image build a self-contained Ray instance suitable
# for end users.

FROM ubuntu:xenial
RUN apt-get update
RUN apt-get install -y vim git wget
RUN apt-get install -y cmake build-essential autoconf curl libtool libboost-all-dev unzip
RUN echo 'export PATH=/opt/conda/bin:$PATH' > /etc/profile.d/conda.sh && \
     wget --quiet 'https://repo.continuum.io/archive/Anaconda2-4.2.0-Linux-x86_64.sh' -O /tmp/anaconda.sh && \
     /bin/bash /tmp/anaconda.sh -b -p /opt/conda && \
     rm /tmp/anaconda.sh
ENV PATH "/opt/conda/bin:$PATH"
RUN conda install libgcc
RUN pip install --upgrade pip
RUN pip install --upgrade "git+git://github.com/cloudpipe/cloudpickle.git@0d225a4695f1f65ae1cbb2e0bbc145e10167cce4"
RUN pip install --upgrade --verbose "git+git://github.com/ray-project/ray.git#egg=numbuf&subdirectory=numbuf"
