# The base-deps Docker image installs main libraries needed to run Ray

FROM ubuntu:xenial
RUN apt-get update \
    && DEBIAN_FRONTEND=noninteractive apt-get install -y --no-install-recommends \
        apt-utils \
        curl \
        g++ \
        git \
        python3-dbg \
        python3-dev \
        python3-pip \
        sudo \
        tzdata \
        wget \
        cmake \
        build-essential \
        unzip \
    && DEBIAN_FRONTEND=noninteractive sudo apt-get install -y libglib2.0-0 \
    && apt-get clean

# Update pip - keep this in a different docker layer to avoid issues:
# Import error: https://github.com/pypa/pip/issues/5599
# Workaround not valid in docker: https://github.com/pypa/pip/issues/5221#issuecomment-381568428
RUN pip3 install --upgrade pip==19.3.1
# Install common pip packages
RUN pip3 install --upgrade setuptools

RUN echo 'export PATH=/opt/conda/bin:$PATH' > /etc/profile.d/conda.sh \
    && wget \
        --quiet 'https://repo.anaconda.com/archive/Anaconda3-2019.10-Linux-x86_64.sh' \
        -O /tmp/anaconda.sh \
    && /bin/bash /tmp/anaconda.sh -b -p /opt/conda \
    && rm /tmp/anaconda.sh \
    && /opt/conda/bin/conda install -y \
        libgcc \
    && /opt/conda/bin/conda clean -y --all
RUN /opt/conda/bin/pip install --upgrade pip==19.3.1
RUN /opt/conda/bin/pip install \
        msgpack==0.6.2 \
        flatbuffers \
        cython==0.29.0 \
        numpy==1.18.0 \
        wrapt==1.11.2 \
        dask==2.10.1 \
        astroid==2.3.3


# To avoid the following error on Jenkins:
# AttributeError: 'numpy.ufunc' object has no attribute '__module__'
# RUN /opt/conda/bin/pip uninstall -y dask

ENV PATH "/opt/conda/bin:$PATH"
