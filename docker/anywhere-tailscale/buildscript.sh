#!/bin/bash
builddir="/home/tripps/build"
#PLATFORM="$( case $(uname --m) in x86_64) echo x64_linux ;; aarch64) echo aarch64_linux ;; esac)"
distribution=$(. /etc/os-release;echo $ID$VERSION_ID)
distro=${distribution//\./}


if [ -d $builddir/ ]; then
    cd $builddir/
    sudo rm -rf *
else
    sudo mkdir -p $builddir /home/tripps/data && cd $builddir 
fi

if [ -x /usr/bin/podman ]; then
    DOCKER_HOST=`echo "unix://${XDG_RUNTIME_DIR}/podman/podman.sock"`
    export DOCKER_HOST
    exec=/usr/bin/podman
else
    exec=/usr/bin/docker
fi

if [ -d /sys/class/power_supply/BAT0 ]; then
    echo "Script is running on a laptop"
    #disable sleep on laptops
    sudo systemctl mask sleep.target suspend.target hibernate.target hybrid-sleep.target
fi

echo "vm.max_map_count = 262144" | sudo tee /etc/sysctl.conf

# Get the total amount of memory in kB
memory=$(grep MemTotal /proc/meminfo | awk '{print $2}')

# Convert kB to GB
gb_memory=$(echo "scale=2; $memory / 1048576" | bc)

shm_memory=$(echo "scale=2; $gb_memory / 3" | bc)

if ! [ -x "$(command -v docker)" ] && ! [ -z "$WSL_DISTRO_NAME" ] && ! [ -x /usr/bin/podman ]; then
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
    sudo apt-key adv --keyserver keyserver.ubuntu.com --recv-keys A4B469963BF863CC
    wget https://developer.download.nvidia.com/compute/cuda/repos/wsl-ubuntu/$(uname --m)/cuda-keyring_1.0-1_all.deb -O cuda-keyring_1.0-1_all.deb && sudo chmod +x cuda-keyring_1.0-1_all.deb
    sudo apt install cuda-keyring_1.0-1_all.deb
    sudo apt update
    sudo apt -y install cuda
    CUDA="WSL"
  elif [ -n "$(lspci | grep -i nvidia)" ]; then
    sudo apt-key adv --keyserver keyserver.ubuntu.com --recv-keys A4B469963BF863CC
    wget https://developer.download.nvidia.com/compute/cuda/repos/$distro/$(uname --m)/cuda-keyring_1.0-1_all.deb -O cuda-keyring_1.0-1_all.deb && sudo chmod +x cuda-keyring_1.0-1_all.deb
    sudo dpkg -i cuda-keyring_1.0-1_all.deb
    sudo apt-get update
    sudo apt-get -y install cuda
    CUDA="lspci"
  fi

  if [[ -n $CUDA ]] && [ -f /usr/local/cuda/version.json ]; then
    # Get the CUDA version
    cuda_version=$(cat /usr/local/cuda/version.json | jq -r '.cuda.version')

    # Strip out the decimal point
    cuda_version=${cuda_version//\./}
    cuda_version=${cuda_version//00/}
  elif [[ -n $CUDA ]] && ! [ -f /usr/local/cuda/version.json ]; then
    #we will default to 11.2
    cuda_version="gpu"
  fi

fi

wget https://raw.githubusercontent.com/jcoffi/cluster-anywhere/master/docker/anywhere-tailscale/Dockerfile -O Dockerfile && wget https://raw.githubusercontent.com/jcoffi/cluster-anywhere/master/docker/anywhere-tailscale/startup.sh -O $builddir/startup.sh && sudo chmod 777 $builddir/Dockerfile && sudo chmod 777 $builddir/startup.sh

if [[ -n $cuda_version ]] && [ $cuda_version != "gpu" ]; then
  sudo $exec build --shm-size=$shm_memory --cache-from=index.docker.io/rayproject/ray-ml:2.1.0-py38-cu$cuda_version $builddir -t jcoffi/cluster-anywhere:cu$cuda_version --build-arg IMAGETYPE=cu$cuda_version
  sudo $exec push jcoffi/cluster-anywhere:cu$cuda_version
elif [ $cuda_version == "gpu" ]; then
  sudo $exec build --shm-size=$shm_memory --cache-from=index.docker.io/rayproject/ray-ml:2.1.0-py38-gpu $builddir -t jcoffi/cluster-anywhere:gpu -t jcoffi/cluster-anywhere:gpu-latest --build-arg IMAGETYPE=gpu
  sudo $exec push jcoffi/cluster-anywhere:gpu
else
  sudo $exec build --shm-size=$shm_memory --cache-from=index.docker.io/rayproject/ray:2.1.0-py38-cpu $builddir -t jcoffi/cluster-anywhere:cpu -t jcoffi/cluster-anywhere:latest -t jcoffi/cluster-anywhere:cpu-latest --build-arg IMAGETYPE=cpu
  sudo $exec push jcoffi/cluster-anywhere:latest
fi 