#!/bin/bash
sudo mkdir -p /home/tripps/build /home/tripps/data && cd /home/tripps/build

# Get the total amount of memory in kB
memory=$(grep MemTotal /proc/meminfo | awk '{print $2}')

# Convert kB to GB
gb_memory=$(echo "scale=2; $memory / 1048576" | bc)

shm_memory=($gb_memory / 3)



# Check if the GPU is NVIDIA
if lspci | grep -i nvidia || [ -n "$WSL_DISTRO_NAME"]; then

  sudo apt install --no-install-recommends -y lspci jq wget
  if [ -x "$(command -v nvidia-smi)" ] && [ -d /usr/local/cuda ]; then
    CUDA=$true
    # Get the driver version
    nvidia_driver_ver=$(nvidia-smi --query-gpu=driver_version --format=csv,noheader)
  else
    sudo apt install --no-install-recommends -y gcc
    wget https://developer.download.nvidia.com/compute/cuda/11.2.0/local_installers/cuda_11.2.0_460.27.04_linux.run -O /home/tripps/build/cuda_11.2.0_460.27.04_linux.run && sudo bash ./cuda_11.2.0_460.27.04_linux.run --silent
    CUDA=$true
    # Get the driver version
    nvidia_driver_ver=$(nvidia-smi --query-gpu=driver_version --format=csv,noheader)
  fi

  if [ -n "$CUDA" ] && [ -f /usr/local/cuda/version.json ]; then
    # Get the CUDA version
    cuda_version=$(cat /usr/local/cuda/version.json | jq -r '.cuda.version')

    # Strip out the decimal point
    cuda_version=${cuda_version//\./}
  else ! [ -f /usr/local/cuda/version.json ] && [ -n "$CUDA" ]
    #we will default to 11.2
    cuda_version=gpu
  fi

fi

wget https://raw.githubusercontent.com/jcoffi/cluster-anywhere/master/docker/anywhere-tailscale/Dockerfile -O /home/tripps/build/Dockerfile && wget https://raw.githubusercontent.com/jcoffi/cluster-anywhere/master/docker/anywhere-tailscale/startup.sh -O /home/tripps/build/startup.sh && sudo chmod 777 /home/tripps/build/Dockerfile && sudo chmod 777 /home/tripps/build/startup.sh

if [ -n "$cuda_version" ] && ! [ $cuda_version = gpu]; then
  sudo docker build --shm-size=$shm_memory --cache-from=index.docker.io/rayproject/ray-ml:2.1.0-py38-cu$cuda_version /home/tripps/build -t jcoffi/cluster-anywhere:cu$cuda_version -t jcoffi/cluster:cu$cuda_version --build-arg IMAGETYPE=cu$cuda_version
elif [ -n "$cuda_version" ] && [ $cuda_version = gpu]; then
  sudo docker build --shm-size=$shm_memory --cache-from=index.docker.io/rayproject/ray-ml:2.1.0-py38-gpu /home/tripps/build -t jcoffi/cluster-anywhere:gpu -t jcoffi/cluster-anywhere:latest-gpu --build-arg IMAGETYPE=gpu
else
  sudo docker build --shm-size=$shm_memory --cache-from=index.docker.io/rayproject/ray:2.1.0-py38-cpu /home/tripps/build -t jcoffi/cluster-anywhere:cpu -t jcoffi/cluster-anywhere:latest -t jcoffi/cluster:cpu-latest --build-arg IMAGETYPE=gpu
fi 