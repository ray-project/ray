# The devcontainer Dockerfile to set up an hermetic enviornment with all needed dependencies.

FROM rayproject/ray-deps:2023-06-26-cu116-aarch64
COPY . /ray
# Install dependencies needed to build ray
RUN sudo apt-get update && sudo apt-get install -y curl unzip cmake gcc g++ && sudo apt-get clean
RUN sudo chown -R ray:users /ray && cd /ray && git init && ./ci/env/install-bazel.sh --system
ENV PATH=$PATH:/home/ray/bin
RUN echo 'build --remote_upload_local_results=false' >> $HOME/.bazelrc 
WORKDIR /ray