#!/bin/bash

# use `bash -i this_file` to run
ROOT_DIR=$(dirname $(dirname $(realpath -s $0)))
echo $ROOT_DIR
MY_IPADDR=172.18.167.27
# source /data3/huangrunhui/anaconda3/etc/profile.d/conda.sh
# source /data3/huangrunhui/anaconda3/bin/activate etc/profile.d/conda.sh
# conda activate rray-gloo3
conda info --envs
echo $MY_IPADDR

ray stop --force
sleep 3
CUDA_VISIBLE_DEVICES=4 ray start --head --object-manager-port=8076 --num-cpus=2 --num-gpus=1 --resources='{"machine":1}'
#--object-store-memory=32359738368
sleep 2

echo "=> node $i"
# export CUDA_VISIBLE_DEVICES=4;
ssh -o StrictHostKeyChecking=no huangrunhui@172.18.167.4 "source /data3/huangrunhui/anaconda3/etc/profile.d/conda.sh; conda activate base; ray stop; CUDA_VISIBLE_DEVICES=4 ray start --address='$MY_IPADDR:6379' --object-manager-port=8076 --num-cpus=2 --num-gpus=1 --resources='{\"machine\":1}'";
# ssh -o StrictHostKeyChecking=no huangrunhui@172.18.167.21 "source /data1/huangrunhui/anaconda3/bin/activate; conda activate ray-build-test; ray stop; ray start --address='$MY_IPADDR:6379' --object-manager-port=8076 --resources='{\"machine\":1}' --object-store-memory=32359738368";
# ssh -o StrictHostKeyChecking=no -i /home/hao.zhang/.ssh/arion.pem hao.zhang@10.20.41.120 "source $ROOT_DIR/env/bin/activate; ray stop; NCCL_DEBUG=INFO NCCL_SOCKET_IFNAME=enp179s0f0 ray start --address='$MY_IPADDR:6379' --object-manager-port=8076 --resources='{\"machine\":1}' --object-store-memory=32359738368";
wait

#sleep 5
#for node in ${OTHERS_IPADDR[@]}; do
# echo "=> $node"
# ssh -o StrictHostKeyChecking=no $node PATH=$PATH:/home/ubuntu/anaconda3/bin:/home/ubuntu/anaconda3/condabin, ray start --redis-address=$MY_IPADDR:6379 --object-manager-port=8076 --resources=\'{\"node\":1}\' --object-store-memory=34359738368 &
#done
#wait
