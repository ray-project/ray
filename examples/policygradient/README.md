How to setup TensorFlow:

First install some prerequisites:

```
sudo apt-get update
sudo apt-get install -y build-essential git linux-headers-generic linux-image-extra-virtual unzip wget pkg-config zip g++ zlib1g-dev libcurl3-dev
```

Install CUDA:

```
wget https://developer.nvidia.com/compute/cuda/8.0/prod/local_installers/cuda-repo-ubuntu1604-8-0-local_8.0.44-1_amd64-deb
sudo dpkg -i cuda-repo-ubuntu1604-8-0-local_8.0.44-1_amd64-deb
sudo apt-get update
sudo apt-get install -y cuda
```

Install CUDNN:

Get cudnn from
https://developer.nvidia.com/rdp/cudnn-download

```
sudo dpkg -i libcudnn5_5.1.10-1+cuda8.0_amd64.deb
sudo dpkg -i libcudnn5-dev_5.1.10-1+cuda8.0_amd64.deb
```

Install TensorFlow:

```
pip install tensorflow-gpu
```
