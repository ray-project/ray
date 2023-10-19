#!/bin/bash

# Configure Linux for Neuron repository updates
. /etc/os-release && \
echo "deb https://apt.repos.neuron.amazonaws.com focal main" | \
sudo tee /etc/apt/sources.list.d/neuron.list > /dev/null

wget -qO - https://apt.repos.neuron.amazonaws.com/GPG-PUB-KEY-AMAZON-AWS-NEURON.PUB | sudo apt-key add -

# Update OS packages 
sudo apt-get update -y

# Install Neuron Runtime 
sudo apt-get install aws-neuronx-collectives=2.* -y
sudo apt-get install aws-neuronx-runtime-lib=2.* -y

# Install Neuron Tools 
sudo apt-get install aws-neuronx-tools=2.* -y

# Install neuronx and torch_xla
pip config set global.extra-index-url https://pip.repos.neuron.amazonaws.com
pip install neuronx-cc==2.* torch-neuronx torchvision
