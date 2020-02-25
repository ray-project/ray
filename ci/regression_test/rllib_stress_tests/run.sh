
source activate tensorflow_p36

python3 wait_cluster.py

rllib train -f atari_impala_xlarge.yaml --ray-address=auto --queue-trials