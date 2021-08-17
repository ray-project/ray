ARG GPU
ARG DOCKER_PREFIX="rayproject/"

# Pull the wheel from the ray-wheel build
FROM "$DOCKER_PREFIX"ray-wheel:nightly"$GPU" AS raywheel
WORKDIR /ray

FROM "$DOCKER_PREFIX"ray-deps:nightly"$GPU"
WORKDIR /ray
# For Click
ENV LC_ALL=C.UTF-8
ENV LANG=C.UTF-8
# Specify bazel (see https://github.com/bazelbuild/bazel/issues/12887)
ENV USE_BAZEL_VERSION=3.7.2
# Sometimes packages.cloud.google.com round robins badly
# See https://github.com/kubernetes/kubernetes/issues/97264 , ugh
RUN (sudo apt-get update || sudo apt-get update || sudo apt-get update) && sudo apt-get install -y curl bash
RUN echo "export PATH=~/anaconda3/bin/:~/.cargo/bin:\$PATH" >> ~/.bashrc
SHELL ["/bin/bash", "-c"]
RUN export PATH=$HOME/.cargo/bin:$HOME/anaconda3/bin/:$PATH
# Install py-spy no arm on conda yet so fall through to pypi wheel
RUN (mamba install --yes py-spy) || (pip install --upgrade pip && pip install py-spy)
RUN mkdir -p /tmp/ray-wheels
COPY --from=raywheel /ray/python/dist/*.whl /tmp/ray-wheels
RUN pip --no-cache-dir install --no-index --find-links="/tmp/ray-wheels/" ray[default]
