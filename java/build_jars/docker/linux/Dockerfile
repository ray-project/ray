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
    && apt-get clean

# To avoid the following error on Jenkins:
# AttributeError: 'numpy.ufunc' object has no attribute '__module__'
RUN /opt/conda/bin/pip uninstall -y dask

ENV PATH "/opt/conda/bin:$PATH"
