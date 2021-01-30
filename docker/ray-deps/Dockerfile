ARG GPU=""
ARG DOCKER_PREFIX
FROM "$DOCKER_PREFIX"base-deps:nightly"$GPU"
# If this arg is not "autoscaler" then no autoscaler requirements will be included
ARG AUTOSCALER="autoscaler"
# For Click
ENV LC_ALL=C.UTF-8
ENV LANG=C.UTF-8
COPY requirements.txt .
# Use multiple cores to compile the C code :)
ENV MAKEFLAGS -j 4

RUN if [ "$AUTOSCALER" == "autoscaler" ]; then \
	$HOME/anaconda3/bin/mamba install --yes six==1.15.0 boto3==1.16.40 google-api-python-client==1.12.8; \
	$HOME/anaconda3/bin/pip --no-cache-dir install  google-oauth kubernetes azure-cli-core==2.22.0 azure-mgmt-compute==14.0.0 azure-mgmt-msi==1.0.0 azure-mgmt-network==10.2.0 azure-mgmt-resource==13.0.0; \
    fi \
    && $HOME/anaconda3/bin/mamba install --yes scipy pandas numba scikit-learn numpy
# Note: there's a patch for TF 2.3.1 on ARM64 CPU - https://github.com/easybuilders/easybuild-easyconfigs/blob/develop/easybuild/easyconfigs/t/TensorFlow/TensorFlow-2.3.1_fix-aarch64.patch
# Since we're on the NVIDIA hardware we can also try https://forums.developer.nvidia.com/t/official-tensorflow-for-jetson-nano/71770
RUN sudo apt-get update && sudo apt-get install -y libhdf5-serial-dev hdf5-tools libhdf5-dev zlib1g-dev zip libjpeg8-dev liblapack-dev libblas-dev gfortran && sudo apt-get clean
# Note: it may be better install from source for optimizations if we set CPP/CFLAGS appropriately, but for now I want this to build & run.
RUN  $HOME/anaconda3/bin/mamba install --yes numpy six requests h5py astor protobuf
# grpcio takes a long time to install from source, lets do it as a seperate stage so it can be cached on re-run.
RUN  $HOME/anaconda3/bin/mamba install --yes grpcio || pip install grpcio
RUN  $HOME/anaconda3/bin/pip install numpy grpcio absl-py py-cpuinfo psutil portpicker six mock requests gast h5py astor termcolor protobuf keras-applications keras-preprocessing wrapt google-pasta pybind11
# Install tensorflow depending on the arch
RUN arch=$(uname -m); \
    echo "Arch is $arch"; \
    if [ "$arch" == "aarch64" ]; then \
	TF_WHEEL="tensorflow-2.3.1+nv20.12-cp36-cp36m-linux_aarch64.whl"; \
	wget -q https://developer.download.nvidia.com/compute/redist/jp/v44/tensorflow/${TF_WHEEL} -O /tmp/${TF_WHEEL}; \
	$HOME/anaconda3/bin/pip install /tmp/${TF_WHEEL} --verbose; \
	rm /tmp/${TF_WHEEL}; \
    else \
	$HOME/anaconda3/bin/pip install "tensorflow==2.3.1"; \
fi;
# Install the generic requirements.
RUN  $HOME/anaconda3/bin/mamba install --yes pyarrow
# Install bazelisk, this is needed for dm_tree on ARM since there is no wheel.
RUN arch=$(uname -m); \
    echo "Arch is $arch"; \
    if [ "$arch" == "aarch64" ]; then \
	BAZEL=bazelisk-linux-arm64; \
    elif [ "$arch" == "x86_64" ]; then \
	BAZEL=bazelisk-linux-amd64; \
    fi; \
    wget -q https://github.com/bazelbuild/bazelisk/releases/download/v1.7.4/${BAZEL} -O /tmp/${BAZEL}; \
    chmod a+x /tmp/${BAZEL};\
    sudo mv /tmp/${BAZEL} /usr/bin/bazel; \
    export PATH=$PATH:/usr/bin;
# Install the opencv as a seperate step for caching happiness
RUN $HOME/anaconda3/bin/pip install opencv-python-headless
RUN $HOME/anaconda3/bin/pip install -r requirements.txt
>>>>>>> ed9b7f24b (Cross-build of base-deps works)
