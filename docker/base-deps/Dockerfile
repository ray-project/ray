# The deploy Docker image build a self-contained Ray instance suitable
# for end users.

FROM ubuntu:xenial
RUN apt-get update \
    && apt-get install -y vim git wget \
    && apt-get install -y cmake pkg-config build-essential autoconf curl libtool unzip flex bison
RUN echo 'export PATH=/opt/conda/bin:$PATH' > /etc/profile.d/conda.sh \
    && wget --quiet 'https://repo.continuum.io/archive/Anaconda2-4.2.0-Linux-x86_64.sh' -O /tmp/anaconda.sh \
    && /bin/bash /tmp/anaconda.sh -b -p /opt/conda \
    && rm /tmp/anaconda.sh
ENV PATH "/opt/conda/bin:$PATH"
RUN conda install -y libgcc
RUN pip install flatbuffers
