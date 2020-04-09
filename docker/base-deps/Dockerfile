# The base-deps Docker image installs main libraries needed to run Ray

FROM ubuntu:xenial
RUN apt-get update \
    && apt-get install -y \
        git \
        wget \
        cmake \
        build-essential \
        curl \
        unzip \
        libgtk2.0-dev \
        zlib1g-dev \
        libgl1-mesa-dev \
    && apt-get clean \
    && echo 'export PATH=/opt/conda/bin:$PATH' > /etc/profile.d/conda.sh \
    && wget \
        --quiet "https://repo.anaconda.com/miniconda/Miniconda3-4.7.12.1-Linux-x86_64.sh" \
        -O /tmp/anaconda.sh \
    && /bin/bash /tmp/anaconda.sh -b -p /opt/conda \
    && rm /tmp/anaconda.sh \
    && /opt/conda/bin/conda install -y \
        libgcc python=3.6.9 \
    && /opt/conda/bin/conda clean -y --all \
    && /opt/conda/bin/pip install \
        flatbuffers \
        cython==0.29.0 \
        numpy==1.15.4
        

# To avoid the following error on Jenkins:
# AttributeError: 'numpy.ufunc' object has no attribute '__module__'
RUN /opt/conda/bin/pip uninstall -y dask

ENV PATH "/opt/conda/bin:$PATH"
