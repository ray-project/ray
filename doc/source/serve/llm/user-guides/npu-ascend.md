(npu-ascend-guide)=
# Recipe for DeepSeek-V4 on NPU via Ray Serve + vLLM

This guide provides a step-by-step recipe for deploying DeepSeek-V4-Flash-w8a8-mtp (w8a8 quantized with multi-token prediction) on Huawei Ascend NPUs using [Ray Serve LLM](https://docs.ray.io/en/latest/serve/tutorials/deployment-serve-llm/medium-size-llm/README.html) and [vLLM-Ascend](https://github.com/vllm-project/vllm-ascend), enabling scalable, efficient, and OpenAI-compatible LLM serving on Ascend NPU hardware.

## Step 1: Download Model Weights

`DeepSeek-V4-Flash-w8a8-mtp` (Quantized version): requires 1 Atlas 800 A3 (128G × 8) node or 1 Atlas 800 A2 (64G × 8) node. [Download model weights](https://www.modelscope.cn/models/Eco-Tech/DeepSeek-V4-Flash-w8a8-mtp)

It is recommended to download the model weights to a shared directory accessible by multiple nodes, such as `/root/.cache/`

## Step 2: Start the Docker Container

This guide demonstrates deployment on an Atlas 800 A2 node. For A3 series deployment, refer to the [vLLM-Ascend DeepSeek-V4-Flash tutorial](https://docs.vllm.ai/projects/ascend/en/v0.18.0/tutorials/models/DeepSeek-V4-Flash.html).

You can use our official Docker image to run `DeepSeek-V4` directly. The image includes the following key dependencies:

| Package | Version |
|---------|---------|
| vllm | 0.18.0 |
| vllm-ascend | 0.17.0 |
| torch | 2.9.0 |
| torch-npu | 2.9.0 |
| transformers | 4.57.6 |
| ray | 2.55.1 |

```sh
export IMAGE=quay.io/ascend/vllm-ascend:deepseekv4
export NAME=vllm-ascend

docker run --rm \
  --name $NAME \
  --net=host \
  --shm-size=512g \
  --device /dev/davinci0 \
  --device /dev/davinci1 \
  --device /dev/davinci2 \
  --device /dev/davinci3 \
  --device /dev/davinci4 \
  --device /dev/davinci5 \
  --device /dev/davinci6 \
  --device /dev/davinci7 \
  --device /dev/davinci_manager \
  --device /dev/devmm_svm \
  --device /dev/hisi_hdc \
  -v /usr/local/dcmi:/usr/local/dcmi \
  -v /usr/local/Ascend/driver/tools/hccn_tool:/usr/local/Ascend/driver/tools/hccn_tool \
  -v /usr/local/bin/npu-smi:/usr/local/bin/npu-smi \
  -v /usr/local/Ascend/driver/lib64/:/usr/local/Ascend/driver/lib64/ \
  -v /usr/local/Ascend/driver/version.info:/usr/local/Ascend/driver/version.info \
  -v /etc/ascend_install.info:/etc/ascend_install.info \
  -v /etc/hccn.conf:/etc/hccn.conf \
  -v /root/.cache:/root/.cache \
  -it $IMAGE bash
```
## Step 3: Set Environment Variables

Set the following environment variables inside the Docker container:

```sh
export LD_PRELOAD=/usr/lib/aarch64-linux-gnu/libjemalloc.so.2:$LD_PRELOAD
export OMP_PROC_BIND=false
export OMP_NUM_THREADS=8
export PYTORCH_NPU_ALLOC_CONF=expandable_segments:True
export ACL_OP_INIT_MODE=1
export VLLM_ASCEND_ENABLE_FLASHCOMM1=1

export USE_MULTI_GROUPS_KV_CACHE=1

export TASK_QUEUE_ENABLE=1
export HCCL_OP_EXPANSION_MODE="AIV"
export HCCL_BUFFSIZE=512

export USE_MULTI_BLOCK_POOL=1
```

> **Optional:** For better NPU performance, run the following kernel parameter tuning commands on the **host machine**:

```sh
sysctl -w vm.swappiness=0
sysctl -w kernel.numa_balancing=0
sysctl kernel.sched_migration_cost_ns=50000
```

## Step 4: Install Ray Serve and Start the Ray Cluster

```sh
pip install "ray[serve]"
ray start --head
```

> **Note:** Make sure to install the latest version of Ray Serve to ensure compatibility with vLLM-Ascend.

Verify that the Ray cluster is running:

```sh
ray status
```

## Step 5: Configure Ray Serve LLM

Create a Python script (e.g., `serve_npu.py`) with the following content. For more details on how to use Ray Serve LLM to deploy LLMs, refer to the [Ray Serve LLM documentation](https://docs.ray.io/en/latest/serve/llm/index.html).

```python
from ray.serve.llm import LLMConfig, build_openai_app
from ray import serve

llm_config = LLMConfig(
    model_loading_config=dict(
        model_id="deepseek-v4-flash",
        model_source="/root/.cache/DeepSeek-V4-Flash-w8a8-mtp",
    ),
    use_cpu=True,
    deployment_config={
        "autoscaling_config": {
            "min_replicas": 1,
            "max_replicas": 1,
        },
        "ray_actor_options": {
            "resources": {"NPU": 8}
        },
    },
    runtime_env={  
        "env_vars": {  
            "VLLM_USE_V1": "1"
        }  
    },
    engine_kwargs=dict(
        tensor_parallel_size=8,
        data_parallel_size=1,
        quantization="ascend",
        enable_expert_parallel=True,
        trust_remote_code=True,
    )
)

app = build_openai_app({"llm_configs": [llm_config]})
serve.run(app, blocking=True)
```

Run the deployment:

```sh
python serve_npu.py
```

## Step 6: Send Requests

You can query the deployed model with cURL:

```sh
curl -X POST http://127.0.0.1:8000/v1/chat/completions \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer fake-key" \
  -d '{
    "model": "deepseek-v4-flash",
    "messages": [{"role": "user", "content": "Hello!"}]
  }'
```