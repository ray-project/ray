FROM ubuntu:focal

RUN apt update -y \
    && DEBIAN_FRONTEND=noninteractive TZ=Etc/UTC apt install -y tzdata npm git curl unzip python3.9 python3.9-dev python3-pip \
    && apt dist-upgrade -y \
    && ln -s /usr/bin/python3.9 /usr/bin/python

RUN git clone --depth=1 --branch wf-cli https://github.com/emre-aydin/ray.git

RUN echo "build --disk_cache=~/bazel-cache --local_ram_resources=HOST_RAM*.5 --local_cpu_resources=4 --jobs=2" >> ~/.bazelrc

RUN cd ray \
    && cd dashboard/client \
    && npm install \
    && npm run build \
    && cd ../.. \
    && ./ci/travis/install-bazel.sh --system \
    && /usr/local/bin/bazel build //:ray_pkg

RUN cd ray/python \
    && pip install --upgrade pip \
    && pip install setuptools \
    && PATH=/usr/local/bin:$PATH python setup.py install