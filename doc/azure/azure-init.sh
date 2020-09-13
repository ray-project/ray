#!/bin/sh

USERNAME=$1
CONDA_ENV=$2
WHEEL=$3
RAY_HEAD_IP=$4
TYPE=$5

echo "Installing wheel..."
sudo -u "$USERNAME" -i /bin/bash -l -c "conda init bash"
sudo -u "$USERNAME" -i /bin/bash -l -c "conda activate $CONDA_ENV; pip install $WHEEL"

echo "Setting up service scripts..."
cat > /home/"$USERNAME"/ray-head.sh << EOM
#!/bin/bash
conda activate $CONDA_ENV

NUM_GPUS=\`nvidia-smi -L | wc -l\`

ray stop
ulimit -n 65536
ray start --head --redis-port=6379 --object-manager-port=8076 --num-gpus=\$NUM_GPUS --block --webui-host 0.0.0.0
EOM

cat > /home/"$USERNAME"/ray-worker.sh << EOM
#!/bin/bash
conda activate $CONDA_ENV

NUM_GPUS=\`nvidia-smi -L | wc -l\`

ray stop
ulimit -n 65536

while true
do
   ray start --address=$RAY_HEAD_IP:6379 --object-manager-port=8076 --num-gpus=\$NUM_GPUS --block
	echo Ray exited. Auto-restarting in 1 second...
	sleep 1
done
EOM

cat > /home/"$USERNAME"/tensorboard.sh << EOM
#!/bin/bash

conda activate $CONDA_ENV
mkdir -p /home/$USERNAME/ray_results

tensorboard --bind_all --logdir=/home/$USERNAME/ray_results
EOM

chmod +x /home/"$USERNAME"/ray-head.sh
chmod +x /home/"$USERNAME"/ray-worker.sh
chmod +x /home/"$USERNAME"/tensorboard.sh

cat > /lib/systemd/system/ray.service << EOM
[Unit]
   Description=Ray

[Service]
   Type=simple
   User=$USERNAME
   ExecStart=/bin/bash -l /home/$USERNAME/ray-$TYPE.sh

[Install]
WantedBy=multi-user.target
EOM

cat > /lib/systemd/system/tensorboard.service << EOM
[Unit]
   Description=TensorBoard

[Service]
   Type=simple
   User=$USERNAME
   ExecStart=/bin/bash -l /home/$USERNAME/tensorboard.sh

[Install]
WantedBy=multi-user.target
EOM

echo "Configure ray to start at boot..."
systemctl enable ray

echo "Starting ray..."
systemctl start ray

# shellcheck disable=SC2154
if [ "$type" = "head" ]; then
   echo "Configure TensorBoard to start at boot..."
   systemctl enable tensorboard

   echo "Starting TensorBoard..."
   systemctl start tensorboard
fi