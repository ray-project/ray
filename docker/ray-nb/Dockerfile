ARG GPU
ARG DOCKER_PREFIX

# We will steal our cross-built wheel from here
FROM "$DOCKER_PREFIX"ray-wheel:nightly"$GPU" AS raywheel
WORKDIR /ray

# This is our actual image that we use as the base
FROM jupyter/base-notebook:lab-3.0.16

RUN conda install -c conda-forge --yes mamba
# For Click
ENV LC_ALL=C.UTF-8
ENV LANG=C.UTF-8
# Specify bazel (see https://github.com/bazelbuild/bazel/issues/12887)
ENV USE_BAZEL_VERSION=3.7.2
USER root
RUN apt-get update && sudo apt-get install -y curl bash
USER $nbuser
RUN echo "export PATH=~/anaconda3/bin/:~/.cargo/bin:\$PATH" >> ~/.bashrc
SHELL ["/bin/bash", "-c"]
RUN export PATH=$HOME/.cargo/bin:$HOME/anaconda3/bin/:$PATH
# Install py-spy no arm on conda yet so fall through to pypi wheel
RUN pip install --upgrade pip
RUN (mamba install --yes py-spy) || (pip install py-spy)
RUN mkdir -p /tmp/ray-wheels
COPY --from=raywheel /ray/python/dist/*.whl /tmp/ray-wheels
RUN pip --no-cache-dir install --no-index --find-links="/tmp/ray-wheels/" ray[all]
