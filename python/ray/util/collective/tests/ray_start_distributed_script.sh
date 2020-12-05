#!/bin/bash
ROOT_DIR=/home/hao.zhang/project/pycharm/ray
echo $ROOT_DIR
MY_IPADDR=10.20.41.115
source $ROOT_DIR/env/bin/activate
echo $MY_IPADDR

ray stop
sleep 0.5
NCCL_DEBUG=INFO NCCL_SOCKET_IFNAME=enp179s0f0 ray start --head --object-manager-port=8076 --resources='{"machine":1}' --object-store-memory=32359738368
sleep 0.5

echo "=> remote node..."
ssh -o StrictHostKeyChecking=no -i /home/hao.zhang/.ssh/arion.pem hao.zhang@10.20.41.120 "source $ROOT_DIR/env/bin/activate; ray stop; NCCL_DEBUG=INFO NCCL_SOCKET_IFNAME=enp179s0f0 ray start --address='$MY_IPADDR:6379' --object-manager-port=8076 --resources='{\"machine\":1}' --object-store-memory=32359738368";
wait
