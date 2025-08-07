# conda create -c conda-forge python=3.9 -n myenv
conda activate myenv

rm -rf /tmp/ray/_serve/
rm -rf /home/arthur/miniforge3/envs/myenv/lib/python3.9/site-packages/ray*

pip install ray[serve] starlette docker httpx
pip install -U https://s3-us-west-2.amazonaws.com/ray-wheels/latest/ray-3.0.0.dev0-cp39-cp39-manylinux2014_x86_64.whl

python python/ray/setup-dev.py -y

# Stop old ray_vllm containers
docker ps -a | grep ray_vllm_ | awk '{print$1}' | xargs docker stop | xargs docker rm
CUDA_VISIBLE_DEVICES=4,5,6,7 ray start --head --port 6980 --temp-dir=/home/arthur/ray


# CUDA_VISIBLE_DEVICES=4,5,6,7  python3 /shared_workspace_mfs/arthur/ray/python/ray/serve/_private/benchmarks/sequential_autoscale_with_vllm.py