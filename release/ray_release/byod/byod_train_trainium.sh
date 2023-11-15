#!/bin/bash
# This script is used to build an extra layer on top of the base anyscale/ray image
# to run the train_torch_trainium_integration test.

sudo apt-get install curl
curl -O https://efa-installer.amazonaws.com/aws-efa-installer-latest.tar.gz
wget https://efa-installer.amazonaws.com/aws-efa-installer.key && gpg --import aws-efa-installer.key
cat aws-efa-installer.key | gpg --fingerprint
wget https://efa-installer.amazonaws.com/aws-efa-installer-latest.tar.gz.sig && gpg --verify ./aws-efa-installer-latest.tar.gz.sig
tar -xvf aws-efa-installer-latest.tar.gz
cd aws-efa-installer && sudo bash efa_installer.sh --yes --skip-kmod
cd
sudo rm -rf aws-efa-installer-latest.tar.gz aws-efa-installer

# Configure Linux for Neuron repository updates
. /etc/os-release && \
echo "deb https://apt.repos.neuron.amazonaws.com ${VERSION_CODENAME} main" | \
sudo tee /etc/apt/sources.list.d/neuron.list > /dev/null

wget -qO - https://apt.repos.neuron.amazonaws.com/GPG-PUB-KEY-AMAZON-AWS-NEURON.PUB | sudo apt-key add -

# Update OS packages 
sudo apt-get update -y

# Install Neuron Runtime 
sudo apt-get install aws-neuronx-collectives=2.* -y
sudo apt-get install aws-neuronx-runtime-lib=2.* -y

# Install Neuron Tools 
sudo apt-get install aws-neuronx-tools=2.* -y

pip config set global.extra-index-url https://pip.repos.neuron.amazonaws.com
pip install neuronx-cc==2.* torch-neuronx torchvision

export FI_PROVIDER=efa
export FI_EFA_USE_DEVICE_RDMA=1
export LD_LIBRARY_PATH=/opt/amazon/efa/lib:$LD_LIBRARY_PATH 
