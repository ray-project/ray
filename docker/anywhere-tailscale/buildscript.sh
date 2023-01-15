#!/bin/bash
sudo mkdir -p /home/tripps/build /home/tripps/data && cd /home/tripps/build

# Get the total amount of memory in kB
memory=$(grep MemTotal /proc/meminfo | awk '{print $2}')

# Convert kB to GB
gb_memory=$(echo "scale=2; $memory / 1048576" | bc)

shm_memory=($gb_memory / 3)

if ! [ -x "$(command -v docker)" ] && ! [ -z "$WSL_DISTRO_NAME" ]; then
sudo curl https://get.docker.com | sh
fi

sudo apt install --no-install-recommends -y jq wget && sudo apt -y autoremove

# Check if the GPU is NVIDIA
if [ -n "$(lspci | grep -i nvidia)" ] || [ -n "$(nvidia-smi -L)" ]; then


  if [ -x "$(command -v nvidia-smi)" ] && [ -d /usr/local/cuda ]; then
    CUDA="Already done"
    # Get the driver version
    nvidia_driver_ver=$(nvidia-smi --query-gpu=driver_version --format=csv,noheader)
  elif [ -n "$WSL_DISTRO_NAME" ]; then
    wget https://developer.download.nvidia.com/compute/cuda/repos/wsl-ubuntu/x86_64/cuda-wsl-ubuntu.pin
    sudo mv cuda-wsl-ubuntu.pin /etc/apt/preferences.d/cuda-repository-pin-600
    sudo apt-key adv --fetch-keys https://developer.download.nvidia.com/compute/cuda/repos/wsl-ubuntu/x86_64/7fa2af80.pub
    sudo add-apt-repository "deb https://developer.download.nvidia.com/compute/cuda/repos/wsl-ubuntu/x86_64/ /"
    sudo apt-get update
    sudo apt-get -y install cuda
    CUDA="WSL"
  elif [ -n "$(lspci | grep -i nvidia)" ]; then
    sudo apt install --no-install-recommends -y gcc
    wget https://developer.download.nvidia.com/compute/cuda/11.2.2/local_installers/cuda_11.2.2_460.32.03_linux.run
    sudo sh cuda_11.2.2_460.32.03_linux.run --silent
    CUDA="lspci"
  fi

  if [[ -n $CUDA ]] && [ -f /usr/local/cuda/version.json ]; then
    # Get the CUDA version
    cuda_version=$(cat /usr/local/cuda/version.json | jq -r '.cuda.version')

    # Strip out the decimal point
    cuda_version=${cuda_version//\./}
  elif [[ -n $CUDA ]] && ! [ -f /usr/local/cuda/version.json ]; then
    #we will default to 11.2
    cuda_version="gpu"
  fi

fi

wget https://raw.githubusercontent.com/jcoffi/cluster-anywhere/master/docker/anywhere-tailscale/Dockerfile -O /home/tripps/build/Dockerfile && wget https://raw.githubusercontent.com/jcoffi/cluster-anywhere/master/docker/anywhere-tailscale/startup.sh -O /home/tripps/build/startup.sh && sudo chmod 777 /home/tripps/build/Dockerfile && sudo chmod 777 /home/tripps/build/startup.sh

if [[ -n $cuda_version ]] && [ $cuda_version != "gpu"]; then
  sudo docker build --shm-size=$shm_memory --cache-from=index.docker.io/rayproject/ray-ml:2.1.0-py38-cu$cuda_version /home/tripps/build -t jcoffi/cluster-anywhere:cu$cuda_version --build-arg IMAGETYPE=cu$cuda_version
elif [[ -n $cuda_version ]] && [ $cuda_version = "gpu"]; then
  sudo docker build --shm-size=$shm_memory --cache-from=index.docker.io/rayproject/ray-ml:2.1.0-py38-gpu /home/tripps/build -t jcoffi/cluster-anywhere:gpu -t jcoffi/cluster-anywhere:gpu-latest --build-arg IMAGETYPE=gpu
else
  sudo docker build --shm-size=$shm_memory --cache-from=index.docker.io/rayproject/ray:2.1.0-py38-cpu /home/tripps/build -t jcoffi/cluster-anywhere:cpu -t jcoffi/cluster-anywhere:latest -t jcoffi/cluster-anywhere:cpu-latest --build-arg IMAGETYPE=cpu
fi 