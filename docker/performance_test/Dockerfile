FROM python:3.6.6-stretch

RUN pip install -q awscli pytest-benchmark
RUN apt-get update \
    && apt-get install -y \
        git \
        wget \
        cmake \
        pkg-config \
        build-essential \
        autoconf \
        curl \
        libtool \
        unzip \
        flex \
        bison \
    && apt-get clean \
    && echo 'export PATH=/opt/conda/bin:$PATH' > /etc/profile.d/conda.sh \
    && wget \
        --quiet 'https://repo.continuum.io/archive/Anaconda2-5.2.0-Linux-x86_64.sh' \
        -O /tmp/anaconda.sh \
    && /bin/bash /tmp/anaconda.sh -b -p /opt/conda \
    && rm /tmp/anaconda.sh \
    && /opt/conda/bin/conda install -y \
        libgcc \
    && /opt/conda/bin/conda clean -y --all \
    && /opt/conda/bin/pip install \
        flatbuffers \
        cython==0.29.0

COPY . .
RUN bash ci/travis/install-bazel.sh
ENV PATH=/root/bin:$PATH

WORKDIR ./python
RUN pip install -e .
WORKDIR ..

ENTRYPOINT ["bash", "ci/jenkins_tests/run_perf_test.sh"]