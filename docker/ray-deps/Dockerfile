ARG GPU=""
ARG DOCKER_PREFIX
FROM "$DOCKER_PREFIX"base-deps:nightly"$GPU"
# If this arg is not "autoscaler" then no autoscaler requirements will be included
ARG AUTOSCALER="autoscaler"
# For Click
ENV LC_ALL=C.UTF-8
ENV LANG=C.UTF-8
# Use multiple cores to compile the C code :)
ENV MAKEFLAGS -j 4

RUN if [ "$AUTOSCALER" == "autoscaler" ]; then \
	$HOME/anaconda3/bin/mamba install --yes six==1.15.0 boto3==1.16.40 google-api-python-client==1.12.8; \
	$HOME/anaconda3/bin/pip --no-cache-dir install  google-oauth kubernetes azure-cli-core==2.22.0 azure-mgmt-compute==14.0.0 azure-mgmt-msi==1.0.0 azure-mgmt-network==10.2.0 azure-mgmt-resource==13.0.0; \
    fi \
    && $HOME/anaconda3/bin/mamba install --yes scipy pandas numba scikit-learn numpy
# Install the generic requirements.
RUN  $HOME/anaconda3/bin/mamba install --yes pyarrow==4.0.1
# Install bazelisk, this is needed for dm_tree on ARM since there is no wheel.
RUN arch=$(uname -m); \
    echo "Arch is $arch"; \
    if [ "$arch" == "aarch64" ]; then \
	BAZEL=bazelisk-linux-arm64; \
    elif [ "$arch" == "x86_64" ]; then \
	BAZEL=bazelisk-linux-amd64; \
    fi; \
    wget -q https://github.com/bazelbuild/bazelisk/releases/download/v1.7.5/${BAZEL} -O /tmp/${BAZEL}; \
    chmod a+x /tmp/${BAZEL};\
    sudo mv /tmp/${BAZEL} /usr/bin/bazel; \
    export PATH=$PATH:/usr/bin;
# Install the opencv as a seperate step for caching happiness
RUN $HOME/anaconda3/bin/pip install opencv-python-headless
COPY requirements.txt .
RUN pip install tensorflow==2.5.1 || pip install https://github.com/bitsy-ai/tensorflow-arm-bin/releases/download/v2.4.0/tensorflow-2.4.0-cp37-none-linux_aarch64.whl
RUN $HOME/anaconda3/bin/pip install -r requirements.txt --extra-index-url=https://www.piwheels.org/simple
