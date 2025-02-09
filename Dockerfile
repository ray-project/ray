# Use the existing Ray GPU image as a base
FROM rayproject/ray:nightly-py310-gpu

# Move over autoscaler changes
COPY ./python/ray/autoscaler /home/ray/anaconda3/lib/python3.10/site-packages/ray/autoscaler/

# Install opengl dependancies
COPY 10_nvidia.json /usr/share/glvnd/egl_vendor.d/10_nvidia.json
RUN sudo apt-get update && DEBIAN_FRONTEND=noninteractive sudo apt-get install -y --no-install-recommends \
    pkg-config \
    libglvnd0 \
    libgl1 \
    libglx0 \
    libegl1 \
    libgles2 \
    libglvnd-dev \
    libgl1-mesa-dev \
    libegl1-mesa-dev \
    libgles2-mesa-dev \
    cmake \
    curl

# nvidia-container-runtime
ENV NVIDIA_DRIVER_CAPABILITIES \
        ${NVIDIA_DRIVER_CAPABILITIES:+$NVIDIA_DRIVER_CAPABILITIES,}graphics,utility

# Default pyopengl to EGL for good headless rendering support
ENV PYOPENGL_PLATFORM egl

# Add nsys# Install nsight-systems-cli
RUN sudo apt update && \
    sudo apt install -y --no-install-recommends gnupg && \
    sudo echo "deb http://developer.download.nvidia.com/devtools/repos/ubuntu$(source /etc/lsb-release; echo "$DISTRIB_RELEASE" | tr -d .)/$(dpkg --print-architecture) /" | sudo tee /etc/apt/sources.list.d/nvidia-devtools.list && \
    sudo apt-key adv --fetch-keys http://developer.download.nvidia.com/compute/cuda/repos/ubuntu1804/x86_64/7fa2af80.pub && \
    sudo apt update && \
    sudo apt install -y --no-install-recommends nsight-systems-cli

# Clean up
RUN apt clean && rm -rf /var/lib/apt/lists/*